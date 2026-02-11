package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.client.NifiNewClient;
import com.emigran.nifi.migration.client.NifiOldClient;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.DataflowSummary;
import com.emigran.nifi.migration.model.Block;
import com.emigran.nifi.migration.model.DiyBlockRequest;
import com.emigran.nifi.migration.model.DiyDataflowRequest;
import com.emigran.nifi.migration.model.Field;
import com.emigran.nifi.migration.model.TransformFlowProperties;
import com.emigran.nifi.migration.model.Workspace;
import com.emigran.nifi.migration.model.WorkspaceCreateRequest;
import com.emigran.nifi.migration.model.DataflowStatus;
import com.emigran.nifi.migration.model.Schedule;
import com.emigran.nifi.migration.util.JsltMappingUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final FlowXmlSecretResolver secretResolver;
    private final FlowXmlTransformPropertiesExtractor transformPropertiesExtractor;
    private final MigrationResultLogger resultLogger;

    /** Fixed block type IDs for transform replacement: convert_csv_to_json, jslt_transform, jolt_transform. */
    private static final int CONVERT_CSV_TO_JSON_BLOCK_ID = 72;
    private static final int JSLT_TRANSFORM_BLOCK_ID = 13820;
    private static final int JOLT_TRANSFORM_BLOCK_ID = 13821;

    @Autowired
    public NifiMigrationService(NifiOldClient oldClient,
                                NifiNewClient newClient,
                                FlowXmlSecretResolver secretResolver,
                                FlowXmlTransformPropertiesExtractor transformPropertiesExtractor,
                                MigrationResultLogger resultLogger) {
        this.oldClient = oldClient;
        this.newClient = newClient;
        this.secretResolver = secretResolver;
        this.transformPropertiesExtractor = transformPropertiesExtractor;
        this.resultLogger = resultLogger;
    }

    private static final Map<String, String> LEGACY_TO_NEW = buildLegacyToNewBlockTypes();

    public String migrateAll() {
        List<Workspace> enabled = oldClient.getWorkspaces()
                .stream()
                .filter(Workspace::isEnabled)
                .filter(w-> w.getId().toString().equals("459"))
                .collect(Collectors.toList());
        for (Workspace ws : enabled) {
            log.info("workspace migration needed");
            migrateWorkspace(ws);
        }
        return resultLogger.getOutputPath();
    }

    private void migrateWorkspace(Workspace workspace) {
        WorkspaceCreateRequest createRequest = new WorkspaceCreateRequest(workspace.getName(), workspace.getOrganisations());
        Workspace newWorkspace;
        try {
            List<Workspace> existing = newClient.getWorkspaces();
            newWorkspace = existing.stream()
                    .filter(ws -> ws.getName() != null && ws.getName().equalsIgnoreCase(workspace.getName()) && ws.isEnabled() )
                    .findFirst()
                    .orElse(null);

            if (newWorkspace != null) {
                log.info("Reusing existing workspace {} -> {}", workspace.getName(), newWorkspace.getId());
            } else {
                newWorkspace = newClient.createWorkspace(createRequest);
                log.info("Created workspace {} -> {}", workspace.getName(),
                        newWorkspace != null ? newWorkspace.getId() : null);
            }
        } catch (Exception ex) {
            resultLogger.logFailure(workspace.getName(), null, "Failed creating/fetching workspace", ex);
            return;
        }

        List<DataflowSummary> liveDataflows = oldClient.getDataflows(workspace.getId())
                .stream()
                .filter(df -> df.getStatus() != null && "Live".equalsIgnoreCase(df.getStatus().getState()))
                .filter(df -> df.getUuid().equalsIgnoreCase("d02641ff-fb75-3d25-85a4-51b728837861"))
                .collect(Collectors.toList());

        for (DataflowSummary summary : liveDataflows) {
            migrateDataflow(workspace, newWorkspace, summary);
        }
    }

    private void migrateDataflow(Workspace sourceWorkspace, Workspace targetWorkspace, DataflowSummary summary) {
        try {
            DataflowDetail detail = oldClient.getDataflowDetail(sourceWorkspace.getId(), summary.getUuid());
            if (detail == null) {
                resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), "Dataflow detail missing", null);
                return;
            }
            secretResolver.resolve(detail, sourceWorkspace.getName(), sourceWorkspace.getUuid(), summary.getUuid());

            TransformContext transformContext = null;
            DiyDataflowRequest diyRequest;
            if (isTransformFlow(detail)) {
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
                    resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(),
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
                diyRequest = buildDiyRequestForTransform(detail, transformContext);
            } else {
                diyRequest = buildDiyRequest(detail);
            }

            DataflowDetail created = newClient.createDiyDataflow(targetWorkspace.getId(), diyRequest);
            String targetUuid = created != null && created.getUuid() != null ? created.getUuid() : summary.getUuid();

            DataflowDetail updatePayload = mergeForUpdate(created != null ? created : detail, detail);
            if (transformContext != null && updatePayload != null && updatePayload.getBlocks() != null) {
                applyTransformContextToBlocks(updatePayload.getBlocks(), transformContext);
            }

            DataflowDetail updated = newClient.updateDataflow(targetWorkspace.getId(), targetUuid, updatePayload);
            Schedule sched = detail.getSchedule();
            if (sched != null) {
                if (sched.getCron() == null && sched.getScheduleExpression() != null) {
                    sched.setCron(sched.getScheduleExpression());
                }
                if (sched.getCron() != null && !sched.getCron().trim().isEmpty()) {
                    newClient.updateSchedule(targetWorkspace.getId(), targetUuid, sched);
                }
            }

            if (updated != null && updated.getStatus() != null) {
                DataflowStatus st = updated.getStatus();
                int disabled = st.getDisabledCount() == null ? 0 : st.getDisabledCount();
                int invalid = st.getInvalidCount() == null ? 0 : st.getInvalidCount();
                if (disabled > 0 || invalid > 0) {
                    resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(),
                            "Post-update status invalid/disabled counts > 0 (disabled=" + disabled + ", invalid=" + invalid + ")", null);
                    return;
                }
            }

            resultLogger.logSuccess(sourceWorkspace.getName(), summary.getName(),
                    "Migrated to workspace " + targetWorkspace.getId() + " with uuid " +
                            (updated != null ? updated.getUuid() : targetUuid));
        } catch (Exception ex) {
            resultLogger.logFailure(sourceWorkspace.getName(), summary.getName(), "Migration failed", ex);
        }
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
        List<DiyBlockRequest> mapped = detail.getBlocks() == null ? Collections.emptyList() :
                detail.getBlocks().stream().map(this::toDiyBlock).collect(Collectors.toList());
        return new DiyDataflowRequest(detail.getName(), mapped);
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
