package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.client.NifiNewClient;
import com.emigran.nifi.migration.client.NifiOldClient;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.DataflowSummary;
import com.emigran.nifi.migration.model.Block;
import com.emigran.nifi.migration.model.DiyBlockRequest;
import com.emigran.nifi.migration.model.DiyDataflowRequest;
import com.emigran.nifi.migration.model.Field;
import com.emigran.nifi.migration.model.Workspace;
import com.emigran.nifi.migration.model.WorkspaceCreateRequest;
import com.emigran.nifi.migration.model.DataflowStatus;
import com.emigran.nifi.migration.model.Schedule;
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
    private final MigrationResultLogger resultLogger;

    @Autowired
    public NifiMigrationService(NifiOldClient oldClient,
                                NifiNewClient newClient,
                                FlowXmlSecretResolver secretResolver,
                                MigrationResultLogger resultLogger) {
        this.oldClient = oldClient;
        this.newClient = newClient;
        this.secretResolver = secretResolver;
        this.resultLogger = resultLogger;
    }

    private static final Map<String, String> LEGACY_TO_NEW = buildLegacyToNewBlockTypes();

    public String migrateAll() {
        List<Workspace> enabled = oldClient.getWorkspaces()
                .stream()
                .filter(Workspace::isEnabled)
                .filter(w-> w.getId().toString().equals("493"))
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
                .filter(df -> df.getUuid().equalsIgnoreCase("2eb514a1-e845-373c-98e2-8ffb617c45a4"))
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
            DiyDataflowRequest diyRequest = buildDiyRequest(detail);
            DataflowDetail created = newClient.createDiyDataflow(targetWorkspace.getId(), diyRequest);
            String targetUuid = created != null && created.getUuid() != null ? created.getUuid() : summary.getUuid();

            DataflowDetail updatePayload = mergeForUpdate(created != null ? created : detail, detail);

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
}
