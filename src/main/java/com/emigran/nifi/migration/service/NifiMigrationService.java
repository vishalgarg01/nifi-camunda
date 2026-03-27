package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.client.ConfigManagerClient;
import com.emigran.nifi.migration.client.NifiNewClient;
import com.emigran.nifi.migration.client.NifiOldClient;
import com.emigran.nifi.migration.client.NeoRuleApiClient;
import com.emigran.nifi.migration.config.MigrationProperties;
import com.emigran.nifi.migration.model.RuleMeta;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.DataflowSummary;
import com.emigran.nifi.migration.model.Block;
import com.emigran.nifi.migration.model.DiyBlockRequest;
import com.emigran.nifi.migration.model.DiyDataflowRequest;
import com.emigran.nifi.migration.model.Field;
import com.emigran.nifi.migration.model.TransformFlowProperties;
import com.emigran.nifi.migration.model.Workspace;
import com.emigran.nifi.migration.model.ProcessorConcurrencyRequest;
import com.emigran.nifi.migration.model.Schedule;
import com.emigran.nifi.migration.model.neo.BlockPosition;
import com.emigran.nifi.migration.model.neo.BlockRelation;
import com.emigran.nifi.migration.model.neo.NeoBlock;
import com.emigran.nifi.migration.model.neo.VersionUpdateRequest;
import com.emigran.nifi.migration.model.configmanager.ConfigResourceRequest;
import com.emigran.nifi.migration.model.configmanager.ConfigResourceResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.emigran.nifi.migration.util.JoltSpecUtil;
import com.emigran.nifi.migration.util.JoltSpecUtil.JoltInputShape;
import com.emigran.nifi.migration.util.JsltMappingUtil;
import com.emigran.nifi.migration.service.FlowXmlConcurrencyExtractor.ProcessorConcurrencyInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class NifiMigrationService {
    private static final Logger log = LoggerFactory.getLogger(NifiMigrationService.class);

    private final NifiOldClient oldClient;
    private final NifiNewClient newClient;
    private final NeoRuleApiClient neoRuleApiClient;
    private final ConfigManagerClient configManagerClient;
    private final MigrationProperties properties;
    private final RulesMetasService rulesMetasService;
    private final FlowXmlSecretResolver secretResolver;
    private final FlowXmlTransformPropertiesExtractor transformPropertiesExtractor;
    private final FlowXmlConcurrencyExtractor flowXmlConcurrencyExtractor;
    private final MigrationResultLogger resultLogger;

    /** Fixed block type IDs for transform replacement: convert_csv_to_json, jslt_transform, jolt_transform. */
    private static final int CONVERT_CSV_TO_JSON_BLOCK_ID = 72;
    private static final int JSLT_TRANSFORM_BLOCK_ID = 13820;
    private static final int JOLT_TRANSFORM_BLOCK_ID = 13821;
    private static final String DEFAULT_HTTP_WRITE_CLIENT_KEY = "EsRqRo3YFndaKhIFeo1DLXcFI";
    private static final String DEFAULT_HTTP_WRITE_CLIENT_SECRET = "62TxYZijwcujh4omFc5NZIwWz8SOomhPB9JyVOGc";
    private static final String DEFAULT_HTTP_WRITE_API_BASE_URL = "https://crm-nightly-new.cc.capillarytech.com";
    private static final String DEFAULT_HTTP_WRITE_OAUTH_BASE_URL = "https://crm-nightly-new.cc.capillarytech.com";
    private static final Set<String> CONFIG_MANAGER_GLOBAL_KEYS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("hostname", "username", "password", "private_key_path", "key_passphrase",
                    "s3BucketName", "s3AccessKey", "s3SecretKey", "dataBricksToken", "clientKey", "clientSecret")));
    private static final ObjectMapper CONFIG_CACHE_MAPPER = new ObjectMapper();

    @Autowired
    public NifiMigrationService(NifiOldClient oldClient,
                                NifiNewClient newClient,
                                NeoRuleApiClient neoRuleApiClient,
                                ConfigManagerClient configManagerClient,
                                MigrationProperties properties,
                                RulesMetasService rulesMetasService,
                                FlowXmlSecretResolver secretResolver,
                                FlowXmlTransformPropertiesExtractor transformPropertiesExtractor,
                                FlowXmlConcurrencyExtractor flowXmlConcurrencyExtractor,
                                MigrationResultLogger resultLogger) {
        this.oldClient = oldClient;
        this.newClient = newClient;
        this.neoRuleApiClient = neoRuleApiClient;
        this.configManagerClient = configManagerClient;
        this.properties = properties;
        this.rulesMetasService = rulesMetasService;
        this.secretResolver = secretResolver;
        this.transformPropertiesExtractor = transformPropertiesExtractor;
        this.flowXmlConcurrencyExtractor = flowXmlConcurrencyExtractor;
        this.resultLogger = resultLogger;
    }

    private static final Map<String, String> LEGACY_TO_NEW = buildLegacyToNewBlockTypes();

    /**
     * Runs migration. If workspaceId is provided, only that workspace is migrated; otherwise all
     * enabled workspaces. If both workspaceId and dataflowId are provided, only that dataflow (or
     * those dataflows) in that workspace are migrated; otherwise all live dataflows in the selected workspace(s).
     *
     * @param workspaceId optional; workspace id (Long as string) or name to migrate; null = all
     * @param dataflowId  optional; single dataflow UUID, or comma-separated list of dataflow UUIDs to migrate; only used when workspaceId is set; null = all
     * @return path to the migration result log
     */
    public String migrateAll(String workspaceId, String dataflowId) {
        log.info("[migrateAll] START - workspaceId={}, dataflowId={}", workspaceId, dataflowId);
        resultLogger.clear();
        List<Workspace> workspaces;
        if (workspaceId != null && !workspaceId.trim().isEmpty()) {
            Workspace single = oldClient.getWorkspace(workspaceId.trim());
            workspaces = single != null ? Collections.singletonList(single) : Collections.emptyList();
            if (workspaces.isEmpty()) {
                log.warn("[migrateAll] Workspace not found for id={}, falling back to list and filter", workspaceId);
                workspaces = oldClient.getWorkspaces()
                        .stream()
                        .filter(Workspace::isEnabled)
                        .filter(ws -> matchesWorkspace(ws, workspaceId))
                        .collect(Collectors.toList());
            }
        } else {
            workspaces = oldClient.getWorkspaces()
                    .stream()
                    .filter(Workspace::isEnabled)
                    .collect(Collectors.toList());
        }
        log.info("[migrateAll] Found {} workspace(s) to migrate", workspaces.size());
        for (Workspace ws : workspaces) {
            if (!ws.isEnabled()) {
                log.warn("[migrateAll] Skipping disabled workspace: {} (id={})", ws.getName(), ws.getId());
                continue;
            }
            try {
                log.info("[migrateAll] Migrating workspace: {} (id={})", ws.getName(), ws.getId());
                migrateWorkspace(ws, dataflowId);
            } catch (Throwable t) {
                log.error("[migrateAll] Workspace {} (id={}) failed, continuing with next: {}",
                        ws.getName(), ws.getId(), t.getMessage(), t);
                resultLogger.logFailure(ws.getName(), "(workspace)", null,
                        "Workspace migration failed: " + (t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName()), t);
            }
        }
        String path = resultLogger.getOutputPath();
        log.info("[migrateAll] END - result log at {}", path);
        return path;
    }

    private static boolean matchesWorkspace(Workspace ws, String workspaceId) {
        if (workspaceId == null || workspaceId.isEmpty()) {
            return true;
        }
        if (ws.getId() != null && workspaceId.trim().equals(ws.getId().toString())) {
            return true;
        }
        return ws.getName() != null && workspaceId.trim().equalsIgnoreCase(ws.getName());
    }

    private void migrateWorkspace(Workspace workspace, String dataflowIdFilter) {
        log.info("[migrateWorkspace] START - workspace={} (id={})", workspace.getName(), workspace.getId());

        List<DataflowSummary> dataflowsToMigrate;
        if (dataflowIdFilter != null && !dataflowIdFilter.trim().isEmpty()) {
            List<String> dataflowUuids = Arrays.stream(dataflowIdFilter.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            List<DataflowSummary> resolved = new ArrayList<>();
            for (String uuid : dataflowUuids) {
                DataflowDetail detail = new DataflowDetail();
                try{
                    detail = oldClient.getDataflowDetail(workspace.getId(), uuid);
                } catch (Exception ex){
                    log.warn("[migrateWorkspace] Dataflow not found: workspaceId={}, dataflowUuid={}", workspace.getId(), uuid);
                }
                if (detail == null) {
                    log.warn("[migrateWorkspace] Dataflow not found: workspaceId={}, dataflowUuid={}", workspace.getId(), uuid);
                } else {
                    resolved.add(detail);
                }
            }
            dataflowsToMigrate = resolved;
        } else {
            log.info("[migrateWorkspace] Fetching live and stopped dataflows for workspace id={}", workspace.getId());
            dataflowsToMigrate = oldClient.getDataflows(workspace.getId())
                    .stream()
                    .filter(df -> df.getStatus() != null &&
                            ("Live".equalsIgnoreCase(df.getStatus().getState()) ||
                             "Stopped".equalsIgnoreCase(df.getStatus().getState())))
                    .collect(Collectors.toList());
        }
        log.info("[migrateWorkspace] Found {} dataflow(s) to migrate", dataflowsToMigrate.size());

        for (DataflowSummary summary : dataflowsToMigrate) {
            try {
                migrateDataflow(workspace, summary);
            } catch (Throwable t) {
                log.error("[migrateWorkspace] Unexpected error migrating dataflow {} (uuid={}), continuing with next: {}",
                        summary.getName(), summary.getUuid(), t.getMessage(), t);
                resultLogger.logFailure(workspace.getName(), summary.getName(), summary.getUuid(),
                        "Unexpected error: " + (t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName()), t);
            }
        }
        log.info("[migrateWorkspace] END - workspace={}", workspace.getName());
    }

    private static boolean matchesDataflow(DataflowSummary df, String dataflowIdFilter) {
        if (dataflowIdFilter == null || dataflowIdFilter.isEmpty()) {
            return true;
        }
        return df.getUuid() != null && dataflowIdFilter.trim().equalsIgnoreCase(df.getUuid());
    }

    private void migrateDataflow(Workspace sourceWorkspace, DataflowSummary summary) {
        log.info("[migrateDataflow] START - dataflow={} (uuid={})", summary.getName(), summary.getUuid());
        try {
            log.info("[migrateDataflow] Fetching dataflow detail from old system");
            DataflowDetail detail = oldClient.getDataflowDetail(sourceWorkspace.getId(), summary.getUuid());
            if (detail == null) {
                log.error("[migrateDataflow] Dataflow detail missing");
                resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), summary.getUuid(), "Dataflow detail missing", null);
                return;
            }
            log.info("[migrateDataflow] Resolving secrets");
            secretResolver.resolve(detail, sourceWorkspace.getName(), sourceWorkspace.getUuid(), summary.getUuid());

            TransformContext transformContext = null;
            boolean isTransform = isTransformFlow(detail);
            if (isTransform) {
                Block transformBlock = detail.getBlocks().stream()
                        .filter(b -> b.getType() != null && (b.getType().startsWith("transform_to_")) || b.getType().startsWith("transfrom_csv_to_rewards") || b.getType().startsWith("retro_template") || b.getType().equalsIgnoreCase("goodwill_points_issue"))
                        .findFirst()
                        .orElse(null);
                String blockNamePrefix = transformBlock != null ? transformBlock.getName() : null;
                TransformFlowProperties transformProps = transformPropertiesExtractor.extract(summary.getUuid(), blockNamePrefix);
                String jsltScript = null;
                String recordGroupBySource = null;
                String sortHeaderSource = null;
                try {
                    if (transformProps.getHeaderMappingJson() != null && !transformProps.getHeaderMappingJson().isEmpty()) {
                        jsltScript = JsltMappingUtil.fromHeaderMappingWithExpressions(
                                transformProps.getHeaderMappingJson(),
                                transformProps.getDateColumnOutputKey(),
                                transformProps.getExistingDateFormat(),
                                transformProps.getNewDateFormat(),
                                transformProps.getTimezoneId());
                    }
                    if (transformProps.getRecordGroupBy() != null && transformProps.getHeaderMappingJson() != null) {
                        recordGroupBySource = JsltMappingUtil.resolveGroupByToSourceNames(
                                transformProps.getRecordGroupBy(),
                                transformProps.getHeaderMappingJson());
                    }
                    if (!isEmptyOrEmptyJson(transformProps.getSortHeaders()) && transformProps.getHeaderMappingJson() != null) {
                        sortHeaderSource = JsltMappingUtil.resolveGroupByToSourceNames(
                                transformProps.getSortHeaders(),
                                transformProps.getHeaderMappingJson());
                    }
                } catch (Exception ex) {
                    log.error("[migrateDataflow] Transform JSLT/group-by generation failed: {}", ex.getMessage());
                    resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                            "Transform JSLT/group-by generation failed", ex);
                    return;
                }
                // When JOLT expects single object {} and group size is 1, CSV block should split response so each record is one JSON.
                boolean splitResponse = transformProps.getJoltSpec() != null
                        && "1".equals(transformProps.getGroupSize())
                        && JoltSpecUtil.getExpectedInputShape(transformProps.getJoltSpec()) == JoltInputShape.OBJECT;
                transformContext = new TransformContext(
                        recordGroupBySource,
                        jsltScript,
                        transformProps.getJoltSpec(),
                        emptyJsonToNull(transformProps.getGroupSize()),
                        sortHeaderSource,
                        transformProps.getAlphabeticalSort(),
                        transformProps.getAttributionType(),
                        transformProps.getAttributionCode(),
                        transformProps.getHeaderValue(),
                        transformProps.getChildTillCode(),
                        transformProps.getChildOrgId(),
                        transformProps.getLineNo(),
                        splitResponse);
            }

            // New flow: create canvas dataflow -> get version -> update with blocks+schedule -> post-hook -> update concurrency
            String neoDataflowId = neoRuleApiClient.createCanvasDataflow(detail.getName());
            if (neoDataflowId == null) {
                log.error("[migrateDataflow] Neo rule API not configured or create failed");
                resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                        "Neo rule API not configured or create canvas failed", null);
                return;
            }

            String versionId = neoRuleApiClient.getFirstVersionId(neoDataflowId);
            if (versionId == null) {
                log.error("[migrateDataflow] Get versions failed");
                resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                        "Get versions failed", null);
                return;
            }

            log.info("[migrateDataflow] Building neo blocks from detail");
            List<NeoBlock> neoBlocks = buildNeoBlocks(detail, transformContext, summary.getUuid());
//            applyHardcodedValuesForNeoBlocks(neoBlocks);
            applyConfigManagerSelectValues(sourceWorkspace,
                    detail != null ? detail.getName() : summary.getName(),
                    summary.getUuid(),
                    neoBlocks);
            String scheduleCron = normalizeToFiveFieldCron(getScheduleCron(detail));

            VersionUpdateRequest updateRequest = new VersionUpdateRequest();
            updateRequest.setBlocks(neoBlocks);
            if(scheduleCron.equalsIgnoreCase("cron_not_set")){
                scheduleCron = "0/2 * * * * ?";
            }
            updateRequest.setSchedule(scheduleCron != null ? scheduleCron : "0 0/5 * * * ?");
            updateRequest.setTag("testingFlows");
            List<String> reportRecipients = newClient.getDataflowReportRecipients(summary.getUuid());
            if (!reportRecipients.isEmpty()) {
                String recipientsCsv = String.join(",", reportRecipients);
                updateRequest.setUsersForReportingEmail(recipientsCsv);
                log.info("[migrateDataflow] Reporting recipients found for dataflow {}: {}", neoDataflowId, recipientsCsv);
            } else {
                log.info("[migrateDataflow] No reporting recipients found for dataflow {}", neoDataflowId);
            }

            log.info("[migrateDataflow] Updating version with {} blocks", neoBlocks.size());
            neoRuleApiClient.updateVersion(neoDataflowId, versionId, updateRequest);

            boolean wasLive = summary.getStatus() != null && "Live".equalsIgnoreCase(summary.getStatus().getState());
            if (!wasLive) {
                log.info("[migrateDataflow] Dataflow was Stopped in source — skipping make-live steps");
                log.info("[migrateDataflow] SUCCESS (stopped) - dataflow={} -> neoDataflowId={}", summary.getName(), neoDataflowId);
                resultLogger.logSuccess(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                        neoDataflowId, "Migrated (stopped) with neo dataflow id " + neoDataflowId);
                return;
            }

            log.info("[migrateDataflow] Post-hook to make dataflow live");
            neoRuleApiClient.postHookForConnectPlus(neoDataflowId, versionId);

            // Build a minimal DataflowDetail with blocks (names only) for concurrency update
            DataflowDetail newDataflowForConcurrency = new DataflowDetail();
            newDataflowForConcurrency.setUuid(neoDataflowId);
            newDataflowForConcurrency.setBlocks(neoBlocks.stream()
                    .map(nb -> {
                        Block b = new Block();
                        b.setName(nb.getName());
                        b.setType(nb.getType());
                        return b;
                    })
                    .collect(Collectors.toList()));

            log.info("[migrateDataflow] Updating processor concurrency from flow.xml");
            updateProcessorConcurrencyFromFlowXml(summary.getUuid(), detail, neoDataflowId, versionId, newDataflowForConcurrency);

            log.info("[migrateDataflow] Starting version in nifi");
            neoRuleApiClient.startVersion(neoDataflowId, versionId);

            log.info("[migrateDataflow] Sending for approval");
            neoRuleApiClient.sendForApproval(neoDataflowId, versionId);

            log.info("[migrateDataflow] Approving version");
            neoRuleApiClient.approveVersion(neoDataflowId, versionId);

            log.info("[migrateDataflow] Making dataflow live");
            neoRuleApiClient.makeLive(neoDataflowId, versionId);

            log.info("[migrateDataflow] SUCCESS - dataflow={} -> neoDataflowId={}", summary.getName(), neoDataflowId);
            resultLogger.logSuccess(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                    neoDataflowId, "Migrated with neo dataflow id " + neoDataflowId);

            try {
                log.info("[migrateDataflow] Stopping old dataflow on source system: workspaceId={}, dataflowUuid={}", sourceWorkspace.getId(), summary.getUuid());
                oldClient.stopDataflow(sourceWorkspace.getId(), summary.getUuid());
            } catch (Exception stopEx) {
                log.warn("[migrateDataflow] Failed to stop old dataflow (non-fatal): {}", stopEx.getMessage());
            }
        } catch (Exception ex) {
            log.error("[migrateDataflow] FAILED - dataflow={}: {}", summary.getName(), ex.getMessage(), ex);
            resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), summary.getUuid(), "Migration failed", ex);
        }
        log.info("[migrateDataflow] END - dataflow={}", summary.getName());
    }

    /**
     * For all processors in the old dataflow (flow.xml) with concurrency != 1: resolve which
     * old block each processor belongs to, find the corresponding new block (same name), and
     * call the glue API to update concurrency on the new block. For InvokeHttp we pass
     * processorType as InvokeHttpV2. Uses neoDataflowId and neoVersionId for the API.
     */
    private void updateProcessorConcurrencyFromFlowXml(String oldDataflowUuid, DataflowDetail oldDetail,
                                                       String neoDataflowId, String neoVersionId, DataflowDetail newDataflow) {
        log.info("[updateProcessorConcurrencyFromFlowXml] START - oldUuid={}, neoDataflowId={}, neoVersionId={}", oldDataflowUuid, neoDataflowId, neoVersionId);
        if (oldDetail == null || oldDetail.getBlocks() == null || newDataflow == null || newDataflow.getBlocks() == null) {
            log.info("[updateProcessorConcurrencyFromFlowXml] Skipping - missing detail or blocks");
            return;
        }
        List<ProcessorConcurrencyInfo> toUpdate = flowXmlConcurrencyExtractor.getProcessorsWithConcurrencyNotOne(oldDataflowUuid);
        log.info("[updateProcessorConcurrencyFromFlowXml] Found {} processor(s) with concurrency != 1", toUpdate.size());
        for (ProcessorConcurrencyInfo info : toUpdate) {
            Block oldBlock = findOldBlockForProcessor(oldDetail.getBlocks(), info.getProcessorName());
            if (oldBlock == null) {
                log.warn("Could not resolve old block for processor {} ({}); skipping concurrency update",
                        info.getProcessorName(), info.getProcessorClass());
                continue;
            }
            Block newBlock = findNewBlockByName(newDataflow.getBlocks(), oldBlock.getName());
            if (newBlock == null) {
                log.warn("No new block with name {} for processor {}; skipping concurrency update",
                        oldBlock.getName(), info.getProcessorName());
                continue;
            }
            String processorTypeForApi = "org.apache.nifi.processors.standard.InvokeHTTP".equals(info.getProcessorClass())
                    ? "com.capillary.foundation.processors.InvokeHttpV2"
                    : info.getProcessorClass();
            ProcessorConcurrencyRequest request = new ProcessorConcurrencyRequest();
            request.setNeoDataflowId(neoDataflowId);
            request.setNeoVersionId(neoVersionId);
            request.setBlockType(newBlock.getType());
            request.setBlockId("");
            request.setBlockName(newBlock.getName());
            request.setProcessorType(processorTypeForApi);
            request.setConcurrentlySchedulableTaskCount(info.getCurrentConcurrency());
            try {
                newClient.updateProcessorConcurrency(request);
                log.info("Updated concurrency to {} for processor {} (old block {} -> new block {})",
                        info.getCurrentConcurrency(), info.getProcessorName(), oldBlock.getName(), newBlock.getName());
            } catch (Exception ex) {
                log.warn("[updateProcessorConcurrencyFromFlowXml] Failed to update concurrency for processor {} (block {}): {}", info.getProcessorName(), newBlock.getName(), ex.getMessage());
            }
        }
        log.info("[updateProcessorConcurrencyFromFlowXml] END");
    }

    /**
     * Finds which old block a processor belongs to by name. Processor names in flow.xml are
     * typically "blockName_suffix" (e.g. connect-to-destination_2). Prefers longest match.
     */
    private Block findOldBlockForProcessor(List<Block> oldBlocks, String processorName) {
        if (processorName == null || processorName.isEmpty() || oldBlocks == null) {
            return null;
        }
        String procLower = processorName.toLowerCase();
        List<Block> byNameLength = oldBlocks.stream()
                .filter(b -> b.getName() != null && !b.getName().isEmpty())
                .sorted((a, b) -> Integer.compare(
                        b.getName().length(), a.getName().length()))
                .collect(Collectors.toList());
        for (Block b : byNameLength) {
            String name = b.getName();
            String nameLower = name.toLowerCase();
            if (procLower.equals(nameLower) || procLower.startsWith(nameLower + "_")) {
                return b;
            }
        }
        return null;
    }

    /** Finds the new dataflow block with the given name (same name as old block after migration). */
    private Block findNewBlockByName(List<Block> newBlocks, String blockName) {
        if (blockName == null || newBlocks == null) {
            return null;
        }
        return newBlocks.stream()
                .filter(b -> blockName.equals(b.getName()))
                .findFirst()
                .orElse(null);
    }

    private boolean isTransformFlow(DataflowDetail detail) {
        if (detail == null || detail.getBlocks() == null) {
            return false;
        }
        return detail.getBlocks().stream()
                .anyMatch(b -> b.getType() != null && (b.getType().startsWith("transform_to_")  || b.getType().startsWith("transfrom_csv_to_rewards") || b.getType().startsWith("retro_template") || b.getType().equalsIgnoreCase("goodwill_points_issue")));
    }

    /** Old block types that map to sftp_read (first block may get Initial Listing Timestamp from API). */
    private static final Set<String> SFTP_READ_LEGACY_TYPES = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("sftp_pull", "sftp_pull_tracing", "sftp_pull_1", "fetch_sftp")));

    /** Block types (new type names) that are Kafka blocks. For these, ${workspaceUuid} / ${workspaceUUID} in config is resolved to old dataflow UUID so consumer group id stays unchanged after migration. */
    private static final Set<String> KAFKA_BLOCK_TYPES = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList("kafka_topic_read","event_notification_read", "kafka_topic_write")));

    /** Kafka connection service IDs: chosen from old block's kafkaBrokers value (event vs connect). */
    private static final String KAFKA_CONNECTION_EVENT = "e82c0901-019a-1000-0000-000024383183";
    private static final String KAFKA_CONNECTION_CONNECT = "e4019edc-019a-1000-0000-0000394c142c";

    /** Alternative field keys/names from old dataflow that map to RulesMetas config keys (e.g. "headerMapping" vs "headersMapping" / "Rename Headers Mapping"). */
    private static final Map<String, List<String>> CONFIG_KEY_FIELD_ALIASES = Collections.unmodifiableMap(
            new HashMap<String, List<String>>() {{
                put("headerMapping", Arrays.asList("headersMapping", "Rename Headers Mapping", "Header Mapping", "input.headerMapping", "input.headersMapping"));
            }});

    /**
     * Builds the list of NeoBlock for the version update API from old dataflow detail.
     * Uses RulesMetas_cps.json for config property names/defaults and LEGACY_TO_NEW for block type mapping.
     * Expands transform_to_* into convert_csv_to_json, jslt_transform, jolt_transform when transformContext is set.
     * When the first block is an SFTP read (sftp_pull, sftp_pull_tracing, sftp_pull_1, fetch_sftp), fetches
     * processed timestamp from the glue API by old dataflow UUID and sets "Initial Listing Timestamp (Migration)" on the new block.
     */
    private List<NeoBlock> buildNeoBlocks(DataflowDetail detail, TransformContext transformContext, String oldDataflowUuid) {
        List<Block> blocks = detail.getBlocks() == null ? Collections.emptyList() : detail.getBlocks();
        List<BlockOrderEntry> ordered = new ArrayList<>();

        Block transformBlock = blocks.stream()
                .filter(b -> b.getType() != null && (b.getType().startsWith("transform_to_")  || b.getType().startsWith("transfrom_csv_to_rewards") || b.getType().startsWith("retro_template") || b.getType().equalsIgnoreCase("goodwill_points_issue")))
                .findFirst()
                .orElse(null);
        int transformOrder = transformBlock != null ? transformBlock.getOrder() : -1;
        String baseName = transformBlock != null ? transformBlock.getName() : null;

        for (Block b : blocks) {
            if (b.getType() != null && (b.getType().startsWith("transform_to_")  || b.getType().startsWith("transfrom_csv_to_rewards") || b.getType().startsWith("retro_template") || b.getType().equalsIgnoreCase("goodwill_points_issue") )) {
                ordered.add(new BlockOrderEntry(baseName + "-csv", mapLegacyToNew("convert_csv_to_json"), transformOrder, b.isSource(), b, "csv"));
                ordered.add(new BlockOrderEntry(baseName + "-jslt", "jslt_transform", transformOrder + 1, false, b, "jslt"));
                ordered.add(new BlockOrderEntry(baseName + "-jolt", "jolt_transform", transformOrder + 2, false, b, "jolt"));
            } else if (transformOrder >= 0 && b.getOrder() > transformOrder) {
                ordered.add(new BlockOrderEntry(b.getName(), mapLegacyToNew(b.getType()), b.getOrder() + 2, b.isSource(), b, null));
            } else {
                ordered.add(new BlockOrderEntry(b.getName(), mapLegacyToNew(b.getType()), b.getOrder(), b.isSource(), b, null));
            }
        }
        ordered.sort((a, b) -> Integer.compare(a.order, b.order));

        // When first block is SFTP read, fetch processed timestamp from glue API (old dataflow) for migration
        String sftpInitialListingTimestamp = null;
        if (!ordered.isEmpty() && oldDataflowUuid != null && !oldDataflowUuid.trim().isEmpty()) {
            BlockOrderEntry first = ordered.get(0);
            if ("sftp_read".equals(first.newType) && first.oldBlock.getType() != null
                    && SFTP_READ_LEGACY_TYPES.contains(first.oldBlock.getType())) {
                try {
                    sftpInitialListingTimestamp = newClient.getListSftpProcessedTimestampByDataflowUuid(oldDataflowUuid.trim());
                    if (sftpInitialListingTimestamp != null) {
                        log.info("[buildNeoBlocks] First block is SFTP read ({}), set Initial Listing Timestamp (Migration) from API: {}",
                                first.oldBlock.getType(), sftpInitialListingTimestamp);
                    }
                } catch (Exception ex) {
                    log.warn("[buildNeoBlocks] Failed to fetch SFTP processed timestamp for dataflow {}: {}",
                            oldDataflowUuid, ex.getMessage());
                }
            }
        }

        // If there is a transform block, read fileDelimiter from it; when not comma, we will set it on the source block (first block)
        String fileDelimiterFromTransform = null;
        if (transformBlock != null && transformBlock.getFields() != null) {
            String fd = getFieldValueFromFields(transformBlock.getFields(), "fileDelimiter", "File Delimiter");
            if (fd != null && !",".equals(fd.trim())) {
                fileDelimiterFromTransform = fd.trim();
            }
        }

        List<NeoBlock> out = new ArrayList<>();
        int positionX = 0;
        for (int i = 0; i < ordered.size(); i++) {
            BlockOrderEntry e = ordered.get(i);
            NeoBlock neo = new NeoBlock();
            neo.setName(e.name);
            neo.setType(e.newType);
            neo.setSource(e.source);
            neo.setPosition(new BlockPosition(positionX, 0));
            positionX += 320;

            Map<String, Object> config = new LinkedHashMap<>(rulesMetasService.getConfigDefaultsForBlockType(e.newType));
            if (e.transformPart != null && transformContext != null) {
                fillConfigFromTransformContext(config, e.transformPart, transformContext);
            } else if (e.oldBlock.getFields() != null) {
                fillConfigFromFields(config, e.oldBlock.getFields());
            }
            if (i == 0 && "sftp_read".equals(e.newType) && sftpInitialListingTimestamp != null) {
                config.put("initialTimestamp", sftpInitialListingTimestamp);
            }
            // If this is the source block (first block) and transform had fileDelimiter != comma, set it on the source block (e.g. sftp_read). Only set when this block's config supports fileDelimiter (so we don't add it to convert_csv_to_json when transform is first).
            if (i == 0 && fileDelimiterFromTransform != null && config.containsKey("File Delimiter")) {
                config.put("File Delimiter", fileDelimiterFromTransform);
                log.debug("[buildNeoBlocks] Source block: set fileDelimiter from old transform block: {}", fileDelimiterFromTransform);
            }
            if ("http_write".equals(e.newType) && e.oldBlock != null && e.oldBlock.getType() != null
                    && e.oldBlock.getType().toLowerCase().startsWith("neo_transformer")) {
                applyNeoTransformerHttpConfig(config, e.oldBlock);
            }
            // For Kafka blocks, resolve ${workspaceUuid}/${workspaceUUID} to old dataflow UUID so consumer group id (and other props) stay the same after migration
            if (KAFKA_BLOCK_TYPES.contains(e.newType) && oldDataflowUuid != null && !oldDataflowUuid.trim().isEmpty()) {
                resolveWorkspaceUuidInConfig(config, oldDataflowUuid.trim());
            }
            // For Kafka blocks, set kafkaConnectionService from old block's kafkaBrokers: "event" -> event connection, "connect" -> connect connection (match anywhere in value)
            if (KAFKA_BLOCK_TYPES.contains(e.newType) && e.oldBlock.getFields() != null) {
                String kafkaConnectionId = resolveKafkaConnectionFromOldBlock(e.oldBlock.getFields());
                if (kafkaConnectionId != null) {
                    config.put("kafkaConnectionService", kafkaConnectionId);
                }
            }
            neo.setConfig(config);

            Map<String, String> varMap = buildVariableConfigKeyMap(config, e.oldBlock.getFields());
            neo.setVariableConfigKeyMap(varMap != null && !varMap.isEmpty() ? varMap : Collections.emptyMap());

            // Relations: sequential order (block i -> block i+1). Explicit chain for transform: csv -> jslt -> jolt -> next
            List<BlockRelation> relations = new ArrayList<>();
            String relationTo = null;
            if ("csv".equals(e.transformPart) && transformBlock != null) {
                relationTo = baseName + "-jslt";
            } else if ("jslt".equals(e.transformPart)) {
                relationTo = baseName + "-jolt";
            } else if (i < ordered.size() - 1) {
                relationTo = ordered.get(i + 1).name;
            }
            if (relationTo != null) {
                relations.add(new BlockRelation(
                        "rel_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10),
                        "isSuccess()",
                        Collections.emptyList(),
                        relationTo));
            }
            neo.setRelations(relations);
            out.add(neo);
        }
        return out;
    }

    private static final class BlockOrderEntry {
        final String name;
        final String newType;
        final int order;
        final boolean source;
        final Block oldBlock;
        final String transformPart; // "csv" | "jslt" | "jolt" | null

        BlockOrderEntry(String name, String newType, int order, boolean source, Block oldBlock, String transformPart) {
            this.name = name;
            this.newType = newType;
            this.order = order;
            this.source = source;
            this.oldBlock = oldBlock;
            this.transformPart = transformPart;
        }
    }

    /** Hardcoded config values for specific Neo block types, applied just before save. */
    private void applyHardcodedValuesForNeoBlocks(List<NeoBlock> neoBlocks) {
        if (neoBlocks == null) return;
        for (NeoBlock neo : neoBlocks) {
            if (neo.getConfig() == null) continue;
            String type = neo.getType();
            if ("http_write".equals(type)) {
                neo.getConfig().put("clientKey", "3Ml3wkihs3YehNDz93rkJ9W45");
                neo.getConfig().put("clientSecret", "E9PL2nbrZ2n3GSQhAPVTskLFCky0mQRxS8iULBBj");

            }
//            else if ("sftp_read".equals(type)) {
//                neo.getConfig().put("username", "capillary");
//                neo.getConfig().put("password", "captech123");
//                neo.getConfig().put("sourceDirectory", "/Capillary testing/vtest4/source");
//                neo.getConfig().put("processedDirectory", "/Capillary testing/vtest4/process");
//                neo.getConfig().put("apiErrorFilePath", "/Capillary testing/vtest4/error");
//            }
        }
    }

    private void applyNeoTransformerHttpConfig(Map<String, Object> config, Block oldBlock) {
        if (config == null) return;

        config.put( "clientKey", DEFAULT_HTTP_WRITE_CLIENT_KEY);
        config.put( "clientSecret", DEFAULT_HTTP_WRITE_CLIENT_SECRET);
//        putIfAbsent(config, "apiBaseUrl", DEFAULT_HTTP_WRITE_API_BASE_URL);
//        putIfAbsent(config, "oAuthBaseUrl", DEFAULT_HTTP_WRITE_OAUTH_BASE_URL);
        config.put( "parseResponse", "false");

        String endpoint = parseNeoDataFlowsEndpoint(oldBlock);
        if (endpoint != null && !endpoint.isEmpty()) {
            Object existing = config.get("apiEndPoint");
            if (existing == null || String.valueOf(existing).trim().isEmpty()) {
                config.put("apiEndPoint", endpoint);
            }
        }
    }

    private String parseNeoDataFlowsEndpoint(Block oldBlock) {
        if (oldBlock == null || oldBlock.getFields() == null) return null;
        String url = getFieldValueFromFields(oldBlock.getFields(), "neoDataFlows", "Neo Data Flows");
        return parseNeoDataFlowsEndpoint(url);
    }

    private String parseNeoDataFlowsEndpoint(String url) {
        if (url == null || url.trim().isEmpty()) return null;
        String path = null;
        try {
            URI uri = new URI(url.trim());
            path = uri.getPath();
        } catch (Exception ignored) {
            path = url.trim();
        }
        if (path == null) return null;
        String prefix = "/api/v1/xto6x/execute";
        String remainder = path.startsWith(prefix) ? path.substring(prefix.length()) : path;
        remainder = remainder == null ? "" : remainder.trim();
        if (remainder.isEmpty()) return null;
        if (!remainder.startsWith("/")) {
            remainder = "/" + remainder;
        }
        if (remainder.startsWith("/x/neo/")) {
            return remainder;
        }
        return "/x/neo" + remainder;
    }

    private void putIfAbsent(Map<String, Object> config, String key, Object value) {
        if (config == null || key == null) return;
        Object existing = config.get(key);
        if (existing == null || String.valueOf(existing).trim().isEmpty()) {
            config.put(key, value);
        }
    }

    private void applyConfigManagerSelectValues(Workspace workspace, String dataflowName, String oldDataflowUuid, List<NeoBlock> neoBlocks) {
        if (neoBlocks == null || neoBlocks.isEmpty()) return;
        String dataflowKey = normalizeForConfigManagerKey(dataflowName);
        ConfigCache cache = loadConfigCache(workspace);
        Map<String, ConfigResourceRequest> requestsByName = new LinkedHashMap<>();
        Set<String> reusableKeys = new HashSet<>();
        List<PendingConfigReference> pending = new ArrayList<>();

        for (NeoBlock neo : neoBlocks) {
            Map<String, Object> config = neo.getConfig();
            if (config == null || config.isEmpty()) continue;
            Map<String, RuleMeta.PropertySchema> schemas = rulesMetasService.getPropertySchemasForBlockType(neo.getType());
            if (schemas.isEmpty()) continue;

            for (Map.Entry<String, RuleMeta.PropertySchema> entry : schemas.entrySet()) {
                String propName = entry.getKey();
                RuleMeta.PropertySchema schema = entry.getValue();
                if (!isConfigManagerSelect(schema)) continue;

                Object raw = config.get(propName);
                if (raw == null) continue;
                String rawValue = String.valueOf(raw);

                if (isMaskedValue(rawValue)) {
                    config.remove(propName);
                    removeVariableKeyMapEntry(neo, propName);
                    continue;
                }

                if (rawValue.contains("___")) {
                    String keyPart = rawValue.substring(0, rawValue.indexOf("___"));
                    ensureVariableKeyMapEntry(neo, propName, keyPart);
                    cache.record(propName, rawValue.substring(rawValue.indexOf("___") + 3), keyPart, oldDataflowUuid);
                    continue;
                }

                boolean isSecret = Boolean.TRUE.equals(schema.getUseSecret());
                String existingKey = cache.find(propName, rawValue);
                if (existingKey != null) {
                    reusableKeys.add(existingKey);
                    pending.add(new PendingConfigReference(neo, propName, existingKey, rawValue, isSecret));
                    cache.record(propName, rawValue, existingKey, oldDataflowUuid);
                    continue;
                }

                boolean propertySeen = cache.hasProperty(propName);
                String base = chooseConfigKeyBase(propName, dataflowKey, propertySeen);
                String configKey = buildConfigManagerKey(base, propName);
                requestsByName.putIfAbsent(configKey, new ConfigResourceRequest(configKey, rawValue, null, isSecret));
                pending.add(new PendingConfigReference(neo, propName, configKey, rawValue, isSecret));
            }
        }

        Set<String> createdKeys = Collections.emptySet();
        if (!requestsByName.isEmpty()) {
            List<ConfigResourceResponse> created = configManagerClient.createConfigs(new ArrayList<>(requestsByName.values()));
            createdKeys = created == null ? Collections.emptySet() :
                    created.stream()
                            .map(ConfigResourceResponse::getConfigName)
                            .collect(Collectors.toSet());
            if (createdKeys.isEmpty()) {
                createdKeys = requestsByName.keySet();
            }
        }
        Set<String> usableKeys = new HashSet<>(reusableKeys);
        usableKeys.addAll(createdKeys);

        for (PendingConfigReference ref : pending) {
            if (!usableKeys.contains(ref.configKey)) {
                continue;
            }
            Map<String, Object> config = ref.block.getConfig();
            if (config != null) {
                String rendered = ref.isSecret ? "*****" : ref.originalValue;
                config.put(ref.propertyName, ref.configKey + "___" + rendered);
            }
            ensureVariableKeyMapEntry(ref.block, ref.propertyName, ref.configKey);
            cache.record(ref.propertyName, ref.originalValue, ref.configKey, oldDataflowUuid);
        }

        saveConfigCache(cache, workspace);
    }

    private boolean isConfigManagerSelect(RuleMeta.PropertySchema schema) {
        if (schema == null || schema.getUiSchema() == null) return false;
        Object component = schema.getUiSchema().get("customComponent");
        return "ConfigManagerSelect".equals(component);
    }

    private String buildConfigManagerKey(String normalizedDataflowName, String propertyName) {
        String prop = propertyName == null || propertyName.trim().isEmpty() ? "config" : propertyName;
        return normalizedDataflowName + "_" + prop;
    }

    private String chooseConfigKeyBase(String propertyName, String dataflowKey, boolean propertySeenBefore) {
        if (propertyName != null && CONFIG_MANAGER_GLOBAL_KEYS.contains(propertyName)
                && properties.getConfigManagerOrgId() != null
                && !properties.getConfigManagerOrgId().trim().isEmpty()) {
            if (propertySeenBefore) {
                return dataflowKey;
            }
            return properties.getConfigManagerOrgId().trim();
        }
        return dataflowKey;
    }

    private String normalizeForConfigManagerKey(String dataflowName) {
        String normalized = dataflowName == null ? "" : dataflowName.trim();
        if (normalized.isEmpty()) {
            normalized = "dataflow";
        }
        normalized = normalized.replaceAll("[^A-Za-z0-9]+", "_");
        normalized = normalized.replaceAll("_+", "_");
        normalized = normalized.replaceAll("^_+|_+$", "");
        return normalized.isEmpty() ? "dataflow" : normalized;
    }

    private void ensureVariableKeyMapEntry(NeoBlock block, String propertyName, String varKey) {
        if (block == null || propertyName == null || varKey == null || varKey.isEmpty()) return;
        Map<String, String> existing = block.getVariableConfigKeyMap();
        Map<String, String> updated = (existing == null || existing.isEmpty())
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(existing);
        updated.put(propertyName, varKey);
        block.setVariableConfigKeyMap(updated);
    }

    private void removeVariableKeyMapEntry(NeoBlock block, String propertyName) {
        if (block == null || propertyName == null) return;
        Map<String, String> existing = block.getVariableConfigKeyMap();
        if (existing == null || existing.isEmpty()) return;
        if (existing.remove(propertyName) != null) {
            block.setVariableConfigKeyMap(new LinkedHashMap<>(existing));
        }
    }

    private boolean isMaskedValue(String rawValue) {
        return rawValue != null && rawValue.matches("\\*+");
    }

    private static final class PendingConfigReference {
        final NeoBlock block;
        final String propertyName;
        final String configKey;
        final String originalValue;
        final boolean isSecret;

        PendingConfigReference(NeoBlock block, String propertyName, String configKey, String originalValue, boolean isSecret) {
            this.block = block;
            this.propertyName = propertyName;
            this.configKey = configKey;
            this.originalValue = originalValue;
            this.isSecret = isSecret;
        }
    }

    private ConfigCache loadConfigCache(Workspace workspace) {
        Path path = getConfigCachePath(workspace);
        if (!Files.exists(path)) {
            return new ConfigCache();
        }
        try {
            String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            if (json == null || json.trim().isEmpty()) {
                return new ConfigCache();
            }
            return CONFIG_CACHE_MAPPER.readValue(json, new TypeReference<ConfigCache>() {});
        } catch (Exception e) {
            log.warn("[ConfigCache] Failed to read cache at {}: {}", path, e.getMessage());
            return new ConfigCache();
        }
    }

    private void saveConfigCache(ConfigCache cache, Workspace workspace) {
        if (cache == null) return;
        Path path = getConfigCachePath(workspace);
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            String json = CONFIG_CACHE_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(cache);
            Files.write(path, json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e) {
            log.warn("[ConfigCache] Failed to write cache at {}: {}", path, e.getMessage());
        }
    }

    private Path getConfigCachePath(Workspace workspace) {
        String baseDir = properties.getFlowXmlCacheDir();
        if (baseDir == null || baseDir.trim().isEmpty()) {
            baseDir = "./flow-cache";
        }
        String workspacePart = workspace != null && workspace.getId() != null ? workspace.getId().toString() : "unknown-workspace";
        return Paths.get(baseDir, "config-manager", "workspace-" + workspacePart + ".json");
    }

    private static final class ConfigCache {
        private Map<String, Map<String, String>> valueIndex = new HashMap<>();
        private List<ConfigCacheEntry> entries = new ArrayList<>();

        String find(String property, String value) {
            if (property == null || value == null) return null;
            Map<String, String> byValue = valueIndex.get(property);
            if (byValue == null) return null;
            return byValue.get(value);
        }

        void record(String property, String value, String configKey, String dataflowUuid) {
            if (property == null || value == null || configKey == null) return;
            valueIndex.computeIfAbsent(property, k -> new HashMap<>()).putIfAbsent(value, configKey);
            entries.add(new ConfigCacheEntry(property, value, configKey, dataflowUuid));
        }

        boolean hasProperty(String property) {
            return property != null && valueIndex.containsKey(property);
        }

        public Map<String, Map<String, String>> getValueIndex() {
            return valueIndex;
        }

        public void setValueIndex(Map<String, Map<String, String>> valueIndex) {
            this.valueIndex = valueIndex;
        }

        public List<ConfigCacheEntry> getEntries() {
            return entries;
        }

        public void setEntries(List<ConfigCacheEntry> entries) {
            this.entries = entries;
        }
    }

    private static final class ConfigCacheEntry {
        public ConfigCacheEntry() {}

        ConfigCacheEntry(String property, String value, String configKey, String dataflowUuid) {
            this.property = property;
            this.value = value;
            this.configKey = configKey;
            this.dataflowUuid = dataflowUuid;
        }

        private String property;
        private String value;
        private String configKey;
        private String dataflowUuid;

        public String getProperty() {
            return property;
        }

        public void setProperty(String property) {
            this.property = property;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getConfigKey() {
            return configKey;
        }

        public void setConfigKey(String configKey) {
            this.configKey = configKey;
        }

        public String getDataflowUuid() {
            return dataflowUuid;
        }

        public void setDataflowUuid(String dataflowUuid) {
            this.dataflowUuid = dataflowUuid;
        }
    }

    /**
     * Returns the value of the first field whose key or name (case-insensitive) matches one of the given keysOrNames.
     */
    private static String getFieldValueFromFields(List<Field> fields, String... keysOrNames) {
        if (fields == null || keysOrNames == null || keysOrNames.length == 0) return null;
        Set<String> match = new HashSet<>();
        for (String k : keysOrNames) {
            if (k != null && !k.isEmpty()) match.add(k.trim().toLowerCase());
        }
        for (Field f : fields) {
            String v = f.getValue();
            if (v == null) continue;
            if (f.getKey() != null && match.contains(f.getKey().toLowerCase())) return v;
            if (f.getName() != null && match.contains(f.getName().toLowerCase())) return v;
        }
        return null;
    }

    private void fillConfigFromFields(Map<String, Object> config, List<Field> fields) {
        if (fields == null) return;
        Map<String, String> byKey = new HashMap<>();
        Map<String, String> byName = new HashMap<>();
        for (Field f : fields) {
            if (f.getKey() != null && f.getValue() != null) byKey.put(f.getKey().toLowerCase(), f.getValue());
            if (f.getName() != null && f.getValue() != null) byName.put(f.getName().toLowerCase(), f.getValue());
        }
        for (String configKey : config.keySet()) {
            String val = byKey.get(configKey.toLowerCase());
            if (val == null) val = byName.get(configKey.toLowerCase());
            if (val == null && CONFIG_KEY_FIELD_ALIASES.containsKey(configKey)) {
                for (String alias : CONFIG_KEY_FIELD_ALIASES.get(configKey)) {
                    if (alias == null) continue;
                    String aliasLower = alias.toLowerCase();
                    val = byKey.get(aliasLower);
                    if (val == null) val = byName.get(aliasLower);
                    if (val != null) break;
                }
            }
            if (val != null) {
                Object parsed = parseConfigValue(config.get(configKey), val);
                config.put(configKey, parsed);
            }
        }
    }

    private Object parseConfigValue(Object defaultVal, String value) {
        if (value == null) return null;
        if (defaultVal instanceof Number) return parseNumber(value);
        if (defaultVal instanceof Boolean) return "true".equalsIgnoreCase(value);
        return value;
    }

    private Number parseNumber(String value) {
        if (value == null) return 0;
        try {
            if (value.contains(".")) return Double.parseDouble(value);
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private void fillConfigFromTransformContext(Map<String, Object> config, String part, TransformContext ctx) {
        if ("csv".equals(part)) {
            if (ctx.getRecordGroupBySource() != null) putByKey(config, "groupBy", ctx.getRecordGroupBySource());
            if (!isEmptyOrEmptyJson(ctx.getGroupSize())) putByKey(config, "groupSize", parseNumber(ctx.getGroupSize()));
            if (!isEmptyOrEmptyJson(ctx.getSortHeaders())) putByKey(config, "sortHeaders", ctx.getSortHeaders());
            if (ctx.getAlphabeticalSort() != null) putByKey(config, "alphabeticalSort", "true".equalsIgnoreCase(ctx.getAlphabeticalSort()));
            if (ctx.getSplitResponse()) config.put("split response", "true");
            if (ctx.getAttributionType() != null) putByKey(config, "attribution_type", ctx.getAttributionType());
            if (ctx.getAttributionCode() != null) putByKey(config, "attribution_code", ctx.getAttributionCode());
            if (ctx.getHeaderValue() != null) putByKey(config, "header_value", ctx.getHeaderValue());
            if (ctx.getChildTillCode() != null) putByKey(config, "child_till_code", ctx.getChildTillCode());
            if (ctx.getChildOrgId() != null) putByKey(config, "child_org_id", ctx.getChildOrgId());
        } else if ("jslt".equals(part) && ctx.getJsltScript() != null) {
            putByKey(config, "transformation", ctx.getJsltScript());
        } else if ("jolt".equals(part) && ctx.getJoltSpec() != null) {
            putByKey(config, "joltTransformation", ctx.getJoltSpec());
        }
    }

    private void putByKey(Map<String, Object> config, String key, Object value) {
        if (config.containsKey(key)) config.put(key, value);
    }

    /** Treats null, empty string, or "{}" as empty so we don't push {} into config (breaks sorting). */
    private static boolean isEmptyOrEmptyJson(String value) {
        if (value == null) return true;
        String t = value.trim();
        return t.isEmpty() || "{}".equals(t);
    }

    /** Returns null when value is null, empty, or "{}"; otherwise returns value. */
    private static String emptyJsonToNull(String value) {
        return isEmptyOrEmptyJson(value) ? null : value;
    }

    /**
     * Replaces ${workspaceUUID} and ${workspaceUuid} in config string values with the resolved old dataflow UUID.
     * Used for Kafka blocks so that consumer group id (and similar props) keep the same value after migration
     * instead of resolving to the new workspace/dataflow and changing behaviour.
     */
    private void resolveWorkspaceUuidInConfig(Map<String, Object> config, String oldDataflowUuid) {
        if (config == null || oldDataflowUuid == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : config.entrySet()) {
            Object val = entry.getValue();

            if (val instanceof String) {
                String s = (String) val;

                String lower = s.toLowerCase();
                if (lower.contains("${workspaceuuid}")) {

                    String resolved = s.replaceAll(
                            "(?i)\\$\\{workspaceuuid\\}",
                            oldDataflowUuid
                    );

                    config.put(entry.getKey(), resolved);

                    log.debug(
                            "[resolveWorkspaceUuidInConfig] Resolved config key {}: {} -> {}",
                            entry.getKey(),
                            s,
                            resolved
                    );
                }
            }
        }
    }

    /**
     * Resolves kafkaConnectionService ID from old block's kafkaBrokers property.
     * If the value contains "event" (anywhere), returns the event connection ID;
     * if it contains "connect" (anywhere), returns the connect connection ID; otherwise null.
     */
    private String resolveKafkaConnectionFromOldBlock(List<Field> fields) {
        String kafkaBrokers = getFieldValueFromFields(fields, "kafkaBrokers", "Kafka Brokers", "brokers");
        if (kafkaBrokers == null || kafkaBrokers.trim().isEmpty()) {
            return null;
        }
        String lower = kafkaBrokers.trim().toLowerCase();
        if (lower.contains("connect")) {
            return KAFKA_CONNECTION_CONNECT;
        }
        if (lower.contains("event")) {
            return KAFKA_CONNECTION_EVENT;
        }
        return null;
    }

    private Map<String, String> buildVariableConfigKeyMap(Map<String, Object> config, List<Field> fields) {
        if (fields == null) return Collections.emptyMap();
        Map<String, String> varMap = new HashMap<>();
        for (Field f : fields) {
            String v = f.getValue();
            if (v != null && v.contains("___")) {
                int i = v.indexOf("___");
                String varKey = v.substring(0, i);
                for (String configKey : config.keySet()) {
                    if (configKey.equalsIgnoreCase(f.getKey()) || (f.getName() != null && configKey.equalsIgnoreCase(f.getName()))) {
                        varMap.put(configKey, varKey);
                        break;
                    }
                }
            }
        }
        return varMap;
    }

    private String getScheduleCron(DataflowDetail detail) {
        Schedule s = detail.getSchedule();
        if (s == null) return null;
        if (s.getCron() != null && !s.getCron().trim().isEmpty()) return s.getCron();
        return s.getScheduleExpression();
    }

    /**
     * If cron has 6 fields (e.g., Quartz with seconds), drop the first field to make it 5-field cron.
     */
    private String normalizeToFiveFieldCron(String cron) {
        if (cron == null) return null;
        String trimmed = cron.trim();
        if (trimmed.isEmpty()) return trimmed;
        String[] parts = trimmed.split("\\s+");
        if (parts.length == 7) {
            return String.join(" ", Arrays.copyOf(parts, 6));
        }
        return trimmed;
    }

    private DiyBlockRequest toDiyBlock(com.emigran.nifi.migration.model.Block block) {
        DiyBlockRequest req = new DiyBlockRequest();
        req.setBlockName(block.getName());
        req.setBlockType(block.getType());
        req.setBlockOrder(block.getOrder());
        req.setBlockId(block.getBlockTypeId());
        return req;
    }

    private DataflowDetail mergeForUpdate(DataflowDetail created, DataflowDetail sourceDetail) {
        log.debug("[mergeForUpdate] Merging created with source detail");
        // Start from created (has ids/blockTypeIds/field metadata) but override values from sourceDetail
        if (created == null || sourceDetail == null || sourceDetail.getBlocks() == null) {
            return sourceDetail != null ? sourceDetail : created;
        }

        Map<String, Block> sourceByName = sourceDetail.getBlocks().stream()
                .collect(Collectors.toMap(b -> b.getName().toLowerCase(), b -> b, (a, b) -> a));

        for (Block createdBlock : created.getBlocks()) {
            Block src = createdBlock.getName() == null ? null : sourceByName.get(createdBlock.getName().toLowerCase());
            if (src == null) {
                continue;
            }
            // Use legacy->new mapping for type
            createdBlock.setType(mapLegacyToNew(src.getType()));
            createdBlock.setSource(src.isSource());

            if (createdBlock.getFields() != null && src.getFields() != null) {
                Map<String, Field> srcFields = src.getFields().stream()
                        .filter(f -> f.getName() != null)
                        .collect(Collectors.toMap(f -> f.getName().toLowerCase(), f -> f, (a, b) -> a));
                for (Field createdField : createdBlock.getFields()) {
                    Field srcField = createdField.getName() == null ? null : srcFields.get(createdField.getName().toLowerCase());
                    if (srcField != null) {
                        createdField.setValue(srcField.getValue());
                    }
                }
            }
        }

        created.setSchedule(sourceDetail.getSchedule());
        created.setStatus(sourceDetail.getStatus());
        return created;
    }

    private String mapLegacyToNew(String legacyType) {
        if (legacyType == null) {
            return null;
        }
        return LEGACY_TO_NEW.getOrDefault(legacyType, legacyType);
    }

    private static Map<String, String> buildLegacyToNewBlockTypes() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("s3_push", "s3_write");
        mapping.put("intouch_transaction_v2", "http_write");
        mapping.put("sftp_pull", "sftp_read");
        mapping.put("sftp_pull_tracing", "sftp_read");
        mapping.put("sftp_pull_1", "sftp_read");
        mapping.put("s3_pull", "s3_read");
        mapping.put("ftp_push", "sftp_write");
        mapping.put("sftp_push", "sftp_write");
        mapping.put("databricks_job_trigger_and_status", "databricks_job_trigger_and_status");
        mapping.put("optional_decrypt_content", "decrypt_content");
        mapping.put("optional_encrypt_content", "encrypt_content");
        mapping.put("csv_to_xml_converter", "convert_csv_to_xml");
        mapping.put("decrypt", "decrypt_content");
        mapping.put("ok_file", "ok_file_3");
        mapping.put("fetch_sftp", "sftp_read");
        mapping.put("sftp_push_hidden", "sftp_write");
        mapping.put("intouch_transaction_v2_1", "http_write");
        mapping.put("retro_destination", "http_write");
        mapping.put("neo_transformer", "http_write");
        mapping.put("kafka_connect_to_source", "kafka_topic_read");
        mapping.put("csv_json_neo_transformer", "convert_csv_to_json");
        mapping.put("neo_transformer_iteration", "neo_block_loop");
        mapping.put("hash_csv_fields", "hash_csv_columns");
        mapping.put("kafka_connect_to_source_tracing", "kafka_topic_read");
        mapping.put("csv_json_neo_transformer_v2", "convert_csv_to_json");
        mapping.put("neo_transformer_v2", "http_write");
        mapping.put("json_split", "split_json");
        mapping.put("sftp_move", "sftp_write");
        mapping.put("put_file_to_sftp", "sftp_write");
        mapping.put("cron_base_trigger", "cron_trigger");
        mapping.put("event_notification_block", "event_notification_read");
        mapping.put("data_validation_block", "databricks_validation");
        mapping.put("databricks_job_trigger_and_status_check", "databricks_job_trigger_and_status_check");
        mapping.put("retro_template", "retro_template");
        mapping.put("goodwill_points_issue", "goodwill_points_issue");
        mapping.put("Convert_CSV/Avro_file_to_Json", "json_to_csv_converter");
        return mapping;
    }

    /**
     * Sets field values on the three transform replacement blocks (72, 100, 101) from the computed context.
     * Matches fields by name (case-insensitive) using common API field names.
     */
    private void applyTransformContextToBlocks(List<Block> blocks, TransformContext ctx) {
        if (blocks == null || ctx == null) {
            return;
        }
        log.debug("[applyTransformContextToBlocks] Applying transform context to {} blocks", blocks.size());
        for (Block block : blocks) {
            int id = block.getBlockTypeId();
            if (block.getFields() == null) {
                continue;
            }
            if (id == CONVERT_CSV_TO_JSON_BLOCK_ID) {
                if (ctx.getRecordGroupBySource() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"Record Group By", "recordGroupBy", "groupBy"}, ctx.getRecordGroupBySource());
                }
                if (!isEmptyOrEmptyJson(ctx.getGroupSize())) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"groupSize", "Minimum Group Record", "Minimum Group Records"}, ctx.getGroupSize());
                }
                if (!isEmptyOrEmptyJson(ctx.getSortHeaders())) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"sortHeaders", "Sort Headers"}, ctx.getSortHeaders());
                }
                if (ctx.getSplitResponse()) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"splitResponse", "Split Response", "split response"}, "true");
                }
                if (ctx.getAlphabeticalSort() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"alphabeticalSort", "Use Alphabetical Sort"}, ctx.getAlphabeticalSort());
                }
                if (ctx.getAttributionType() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"attribution_type", "attributionType"}, ctx.getAttributionType());
                }
                if (ctx.getAttributionCode() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"attribution_code", "attributionCode"}, ctx.getAttributionCode());
                }
                if (ctx.getHeaderValue() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"header_value", "headerValue"}, ctx.getHeaderValue());
                }
                if (ctx.getChildTillCode() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"child_till_code", "childTillCode"}, ctx.getChildTillCode());
                }
                if (ctx.getChildOrgId() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"child_org_id", "childOrgId"}, ctx.getChildOrgId());
                }
            } else if (id == JSLT_TRANSFORM_BLOCK_ID && ctx.getJsltScript() != null) {
                setFieldValueByKeyword(block.getFields(), new String[]{"transformation", "JSLT Transform", "JSLT Script", "jsltScript", "transform"}, ctx.getJsltScript());
            } else if (id == JOLT_TRANSFORM_BLOCK_ID && ctx.getJoltSpec() != null) {
                setFieldValueByKeyword(block.getFields(), new String[]{"joltTransformation", "Jolt Specification", "joltSpec", "Jolt Spec"}, ctx.getJoltSpec());
            }
        }
    }

    private void setFieldValueByKeyword(List<Field> fields, String[] keywords, String value) {
        for (Field f : fields) {
            if (f.getName() == null) {
                continue;
            }
            String nameLower = f.getName().toLowerCase();
            for (String kw : keywords) {
                if (kw != null && nameLower.contains(kw.toLowerCase())) {
                    f.setValue(value);
                    return;
                }
            }
        }
    }

    private static final class TransformContext {
        private final String recordGroupBySource;
        private final String jsltScript;
        private final String joltSpec;
        private final String groupSize;
        private final String sortHeaders;
        private final String alphabeticalSort;
        private final String attributionType;
        private final String attributionCode;
        private final String headerValue;
        private final String childTillCode;
        private final String childOrgId;
        private final String lineNo;
        private final boolean splitResponse;

        TransformContext(String recordGroupBySource, String jsltScript, String joltSpec,
                         String groupSize, String sortHeaders, String alphabeticalSort,
                         String attributionType, String attributionCode, String headerValue,
                         String childTillCode, String childOrgId, String lineNo,
                         boolean splitResponse) {
            this.recordGroupBySource = recordGroupBySource;
            this.jsltScript = jsltScript;
            this.joltSpec = joltSpec;
            this.groupSize = groupSize;
            this.sortHeaders = sortHeaders;
            this.alphabeticalSort = alphabeticalSort;
            this.attributionType = attributionType;
            this.attributionCode = attributionCode;
            this.headerValue = headerValue;
            this.childTillCode = childTillCode;
            this.childOrgId = childOrgId;
            this.lineNo = lineNo;
            this.splitResponse = splitResponse;
        }

        String getRecordGroupBySource() {
            return recordGroupBySource;
        }

        String getJsltScript() {
            return jsltScript;
        }

        String getJoltSpec() {
            return joltSpec;
        }

        String getGroupSize() {
            return groupSize;
        }

        String getSortHeaders() {
            return sortHeaders;
        }

        String getAlphabeticalSort() {
            return alphabeticalSort;
        }

        String getAttributionType() {
            return attributionType;
        }

        String getAttributionCode() {
            return attributionCode;
        }

        String getHeaderValue() {
            return headerValue;
        }

        String getChildTillCode() {
            return childTillCode;
        }

        String getChildOrgId() {
            return childOrgId;
        }

        String getLineNo() {
            return lineNo;
        }

        boolean getSplitResponse() {
            return splitResponse;
        }
    }
}
