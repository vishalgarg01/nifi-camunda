package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.client.NifiNewClient;
import com.emigran.nifi.migration.client.NifiOldClient;
import com.emigran.nifi.migration.client.NeoRuleApiClient;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.DataflowSummary;
import com.emigran.nifi.migration.model.Block;
import com.emigran.nifi.migration.model.DiyBlockRequest;
import com.emigran.nifi.migration.model.DiyDataflowRequest;
import com.emigran.nifi.migration.model.Field;
import com.emigran.nifi.migration.model.TransformFlowProperties;
import com.emigran.nifi.migration.model.Workspace;
import com.emigran.nifi.migration.model.WorkspaceCreateRequest;
import com.emigran.nifi.migration.model.ProcessorConcurrencyRequest;
import com.emigran.nifi.migration.model.Schedule;
import com.emigran.nifi.migration.model.neo.BlockPosition;
import com.emigran.nifi.migration.model.neo.BlockRelation;
import com.emigran.nifi.migration.model.neo.NeoBlock;
import com.emigran.nifi.migration.model.neo.VersionUpdateRequest;
import com.emigran.nifi.migration.util.JsltMappingUtil;
import com.emigran.nifi.migration.service.FlowXmlConcurrencyExtractor.ProcessorConcurrencyInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
    private final RulesMetasService rulesMetasService;
    private final FlowXmlSecretResolver secretResolver;
    private final FlowXmlTransformPropertiesExtractor transformPropertiesExtractor;
    private final FlowXmlConcurrencyExtractor flowXmlConcurrencyExtractor;
    private final MigrationResultLogger resultLogger;

    /** Fixed block type IDs for transform replacement: convert_csv_to_json, jslt_transform, jolt_transform. */
    private static final int CONVERT_CSV_TO_JSON_BLOCK_ID = 72;
    private static final int JSLT_TRANSFORM_BLOCK_ID = 13820;
    private static final int JOLT_TRANSFORM_BLOCK_ID = 13821;

    @Autowired
    public NifiMigrationService(NifiOldClient oldClient,
                                NifiNewClient newClient,
                                NeoRuleApiClient neoRuleApiClient,
                                RulesMetasService rulesMetasService,
                                FlowXmlSecretResolver secretResolver,
                                FlowXmlTransformPropertiesExtractor transformPropertiesExtractor,
                                FlowXmlConcurrencyExtractor flowXmlConcurrencyExtractor,
                                MigrationResultLogger resultLogger) {
        this.oldClient = oldClient;
        this.newClient = newClient;
        this.neoRuleApiClient = neoRuleApiClient;
        this.rulesMetasService = rulesMetasService;
        this.secretResolver = secretResolver;
        this.transformPropertiesExtractor = transformPropertiesExtractor;
        this.flowXmlConcurrencyExtractor = flowXmlConcurrencyExtractor;
        this.resultLogger = resultLogger;
    }

    private static final Map<String, String> LEGACY_TO_NEW = buildLegacyToNewBlockTypes();

    /**
     * Runs migration. If workspaceId is provided, only that workspace is migrated; otherwise all
     * enabled workspaces. If both workspaceId and dataflowId are provided, only that dataflow in
     * that workspace is migrated; otherwise all live dataflows in the selected workspace(s).
     *
     * @param workspaceId optional; workspace id (Long as string) or name to migrate; null = all
     * @param dataflowId  optional; dataflow UUID to migrate; only used when workspaceId is set; null = all
     * @return path to the migration result log
     */
    public String migrateAll(String workspaceId, String dataflowId) {
        log.info("[migrateAll] START - workspaceId={}, dataflowId={}", workspaceId, dataflowId);
        resultLogger.clear();
        List<Workspace> workspaces = oldClient.getWorkspaces()
                .stream()
                .filter(Workspace::isEnabled)
                .filter(ws -> matchesWorkspace(ws, workspaceId))
                .collect(Collectors.toList());
        log.info("[migrateAll] Found {} workspace(s) to migrate", workspaces.size());
        for (Workspace ws : workspaces) {
            log.info("[migrateAll] Migrating workspace: {} (id={})", ws.getName(), ws.getId());
            migrateWorkspace(ws, dataflowId);
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
        WorkspaceCreateRequest createRequest = new WorkspaceCreateRequest(workspace.getName(), workspace.getOrganisations());
        Workspace newWorkspace;
        try {
            log.info("[migrateWorkspace] Fetching/creating target workspace");
            List<Workspace> existing = newClient.getWorkspaces();
            newWorkspace = existing.stream()
                    .filter(ws -> ws.getName() != null && ws.getName().equalsIgnoreCase(workspace.getName()) && ws.isEnabled() )
                    .findFirst()
                    .orElse(null);

            if (newWorkspace != null) {
                log.info("[migrateWorkspace] Reusing existing workspace {} -> {}", workspace.getName(), newWorkspace.getId());
            } else {
                newWorkspace = newClient.createWorkspace(createRequest);
                log.info("[migrateWorkspace] Created workspace {} -> {}", workspace.getName(),
                        newWorkspace != null ? newWorkspace.getId() : null);
            }
        } catch (Exception ex) {
            log.error("[migrateWorkspace] Failed creating/fetching workspace: {}", ex.getMessage());
            resultLogger.logFailure(workspace.getName(), null, "Failed creating/fetching workspace", ex);
            return;
        }

        log.info("[migrateWorkspace] Fetching live dataflows for workspace id={}", workspace.getId());
        List<DataflowSummary> liveDataflows = oldClient.getDataflows(workspace.getId())
                .stream()
                .filter(df -> df.getStatus() != null && "Live".equalsIgnoreCase(df.getStatus().getState()))
                .filter(df -> matchesDataflow(df, dataflowIdFilter))
                .collect(Collectors.toList());
        log.info("[migrateWorkspace] Found {} dataflow(s) to migrate", liveDataflows.size());

        for (DataflowSummary summary : liveDataflows) {
            migrateDataflow(workspace, newWorkspace, summary);
        }
        log.info("[migrateWorkspace] END - workspace={}", workspace.getName());
    }

    private static boolean matchesDataflow(DataflowSummary df, String dataflowIdFilter) {
        if (dataflowIdFilter == null || dataflowIdFilter.isEmpty()) {
            return true;
        }
        return df.getUuid() != null && dataflowIdFilter.trim().equalsIgnoreCase(df.getUuid());
    }

    private void migrateDataflow(Workspace sourceWorkspace, Workspace targetWorkspace, DataflowSummary summary) {
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
                        .filter(b -> b.getType() != null && b.getType().startsWith("transform_to_"))
                        .findFirst()
                        .orElse(null);
                String blockNamePrefix = transformBlock != null ? transformBlock.getName() : null;
                TransformFlowProperties transformProps = transformPropertiesExtractor.extract(summary.getUuid(), blockNamePrefix);
                String jsltScript = null;
                String recordGroupBySource = null;
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
                } catch (Exception ex) {
                    log.error("[migrateDataflow] Transform JSLT/group-by generation failed: {}", ex.getMessage());
                    resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                            "Transform JSLT/group-by generation failed", ex);
                    return;
                }
                transformContext = new TransformContext(
                        recordGroupBySource,
                        jsltScript,
                        transformProps.getJoltSpec(),
                        transformProps.getGroupSize(),
                        transformProps.getSortHeaders(),
                        transformProps.getAlphabeticalSort(),
                        transformProps.getAttributionType(),
                        transformProps.getAttributionCode(),
                        transformProps.getHeaderValue(),
                        transformProps.getChildTillCode(),
                        transformProps.getChildOrgId());
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
            List<NeoBlock> neoBlocks = buildNeoBlocks(detail, transformContext);
            String scheduleCron = getScheduleCron(detail);

            VersionUpdateRequest updateRequest = new VersionUpdateRequest();
            updateRequest.setBlocks(neoBlocks);
            updateRequest.setSchedule(scheduleCron != null ? scheduleCron : "0 0/5 * * * ?");
            updateRequest.setTag("migration");

            log.info("[migrateDataflow] Updating version with {} blocks", neoBlocks.size());
            neoRuleApiClient.updateVersion(neoDataflowId, versionId, updateRequest);

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
//            neoRuleApiClient.startVersion(neoDataflowId, versionId);

            log.info("[migrateDataflow] Sending for approval");
            neoRuleApiClient.sendForApproval(neoDataflowId, versionId);

            log.info("[migrateDataflow] Approving version");
            neoRuleApiClient.approveVersion(neoDataflowId, versionId);

            log.info("[migrateDataflow] Making dataflow live");
            neoRuleApiClient.makeLive(neoDataflowId, versionId);

            log.info("[migrateDataflow] SUCCESS - dataflow={} -> neoDataflowId={}", summary.getName(), neoDataflowId);
            resultLogger.logSuccess(sourceWorkspace.getName(), summary.getName(), summary.getUuid(),
                    "Migrated with neo dataflow id " + neoDataflowId);
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
                .anyMatch(b -> b.getType() != null && b.getType().startsWith("transform_to_"));
    }

    /**
     * Builds DIY request for a transform flow: replaces the single transform_to_* block with
     * convert_csv_to_json(72), jslt_transform(100), jolt_transform(101). Blocks after the
     * transform get blockOrder increased by 2.
     */
    private DiyDataflowRequest buildDiyRequestForTransform(DataflowDetail detail, TransformContext transformContext) {
        log.debug("[buildDiyRequestForTransform] Building DIY request with transform replacement blocks");
        List<Block> blocks = detail.getBlocks() == null ? Collections.emptyList() : detail.getBlocks();
        Block transformBlock = blocks.stream()
                .filter(b -> b.getType() != null && b.getType().startsWith("transform_to_"))
                .findFirst()
                .orElse(null);
        if (transformBlock == null) {
            return buildDiyRequest(detail);
        }
        int transformOrder = transformBlock.getOrder();
        String baseName = transformBlock.getName();

        List<DiyBlockRequest> out = new ArrayList<>();
        for (Block b : blocks) {
            if (b.getType() != null && b.getType().startsWith("transform_to_")) {
                out.add(diyBlock(baseName + "-csv", "convert_csv_to_json", transformOrder, CONVERT_CSV_TO_JSON_BLOCK_ID));
                out.add(diyBlock(baseName + "-jslt", "jslt_transform", transformOrder + 1, JSLT_TRANSFORM_BLOCK_ID));
                out.add(diyBlock(baseName + "-jolt", "jolt_transform", transformOrder + 2, JOLT_TRANSFORM_BLOCK_ID));
            } else if (b.getOrder() > transformOrder) {
                DiyBlockRequest r = toDiyBlock(b);
                r.setBlockOrder(b.getOrder() + 2);
                out.add(r);
            } else {
                out.add(toDiyBlock(b));
            }
        }
        return new DiyDataflowRequest(detail.getName(), out);
    }

    private DiyBlockRequest diyBlock(String name, String type, int order, int blockId) {
        DiyBlockRequest req = new DiyBlockRequest();
        req.setBlockName(name);
        req.setBlockType(type);
        req.setBlockOrder(order);
        req.setBlockId(blockId);
        return req;
    }

    private DiyDataflowRequest buildDiyRequest(DataflowDetail detail) {
        log.debug("[buildDiyRequest] Building DIY request with {} blocks", detail.getBlocks() == null ? 0 : detail.getBlocks().size());
        List<DiyBlockRequest> mapped = detail.getBlocks() == null ? Collections.emptyList() :
                detail.getBlocks().stream().map(this::toDiyBlock).collect(Collectors.toList());
        return new DiyDataflowRequest(detail.getName(), mapped);
    }

    /**
     * Builds the list of NeoBlock for the version update API from old dataflow detail.
     * Uses RulesMetas_cps.json for config property names/defaults and LEGACY_TO_NEW for block type mapping.
     * Expands transform_to_* into convert_csv_to_json, jslt_transform, jolt_transform when transformContext is set.
     */
    private List<NeoBlock> buildNeoBlocks(DataflowDetail detail, TransformContext transformContext) {
        List<Block> blocks = detail.getBlocks() == null ? Collections.emptyList() : detail.getBlocks();
        List<BlockOrderEntry> ordered = new ArrayList<>();

        Block transformBlock = blocks.stream()
                .filter(b -> b.getType() != null && b.getType().startsWith("transform_to_"))
                .findFirst()
                .orElse(null);
        int transformOrder = transformBlock != null ? transformBlock.getOrder() : -1;
        String baseName = transformBlock != null ? transformBlock.getName() : null;

        for (Block b : blocks) {
            if (b.getType() != null && b.getType().startsWith("transform_to_")) {
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
            if (ctx.getGroupSize() != null) putByKey(config, "groupSize", parseNumber(ctx.getGroupSize()));
            if (ctx.getSortHeaders() != null) putByKey(config, "sortHeaders", ctx.getSortHeaders());
            if (ctx.getAlphabeticalSort() != null) putByKey(config, "alphabeticalSort", "true".equalsIgnoreCase(ctx.getAlphabeticalSort()));
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
        mapping.put("neo_transformer", "neo_block");
        mapping.put("kafka_connect_to_source", "kafka_read");
        mapping.put("csv_json_neo_transformer", "convert_csv_to_json");
        mapping.put("neo_transformer_iteration", "neo_block_loop");
        mapping.put("hash_csv_fields", "hash_csv_columns");
        mapping.put("kafka_connect_to_source_tracing", "kafka_read");
        mapping.put("csv_json_neo_transformer_v2", "convert_csv_to_json");
        mapping.put("neo_transformer_v2", "neo_block");
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
                if (ctx.getGroupSize() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"groupSize", "Minimum Group Record", "Minimum Group Records"}, ctx.getGroupSize());
                }
                if (ctx.getSortHeaders() != null) {
                    setFieldValueByKeyword(block.getFields(), new String[]{"sortHeaders", "Sort Headers"}, ctx.getSortHeaders());
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

        TransformContext(String recordGroupBySource, String jsltScript, String joltSpec,
                        String groupSize, String sortHeaders, String alphabeticalSort,
                        String attributionType, String attributionCode, String headerValue,
                        String childTillCode, String childOrgId) {
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
    }
}
