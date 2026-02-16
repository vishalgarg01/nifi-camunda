package com.emigran.nifi.migration.client;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.emigran.nifi.migration.model.ProcessedTimestampResponse;
import com.emigran.nifi.migration.model.ProcessorConcurrencyRequest;
import com.emigran.nifi.migration.model.Workspace;
import com.emigran.nifi.migration.model.WorkspaceCreateRequest;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.DiyDataflowRequest;
import com.emigran.nifi.migration.model.Schedule;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class NifiNewClient {

    private final NifiApiClient client;

    @Autowired
    public NifiNewClient(MigrationProperties properties) {
        RestTemplate restTemplate = new RestTemplate();
        this.client = new NifiApiClient(restTemplate, properties.getNewBaseUrl(), properties.getBasicAuthToken(),
                properties.getSourceHeader());
    }

    public Workspace createWorkspace(WorkspaceCreateRequest request) {
        ResponseEntity<Workspace> response = client.post("/workspaces", request, true, Workspace.class);
        return response.getBody();
    }

    public List<Workspace> getWorkspaces() {
        ResponseEntity<Workspace[]> response = client.get("/workspaces",true,  Workspace[].class);
        Workspace[] body = response.getBody();
        return body == null ? Collections.emptyList() : Arrays.asList(body);
    }


    public DataflowDetail createDiyDataflow(Long workspaceId, DiyDataflowRequest request) {
        ResponseEntity<DataflowDetail> response = client.post(
                "/workspaces/" + workspaceId + "/dataflows/diy", request, true, DataflowDetail.class);
        return response.getBody();
    }

    public DataflowDetail updateDataflow(Long workspaceId, String uuid, DataflowDetail request) {
        ResponseEntity<DataflowDetail> response = client.put(
                "/workspaces/" + workspaceId + "/dataflows/" + uuid, request, true, DataflowDetail.class);
        return response.getBody();
    }

    public Schedule updateSchedule(Long workspaceId, String uuid, Schedule schedule) {
        ResponseEntity<Schedule> response = client.put(
                "/workspaces/" + workspaceId + "/dataflows/" + uuid + "/schedule", schedule, true, Schedule.class);
        return response.getBody();
    }

    /**
     * Updates the concurrency (concurrentlySchedulableTaskCount) for a processor in a dataflow.
     * PUT /api/processors/concurrency
     */
    public void updateProcessorConcurrency(ProcessorConcurrencyRequest request) {
        client.put("/processors/concurrency", request, true, Void.class);
    }

    /**
     * Gets the processed timestamp from the first ListSFTP processor in the dataflow (old system).
     * GET /api/processors/list-sftp-processed-timestamp-by-dataflow?dataflowUuid={dataflowUuid}
     *
     * @param dataflowUuid old dataflow UUID
     * @return processedTimestamp value, or null if not found or on error
     */
    public String getListSftpProcessedTimestampByDataflowUuid(String dataflowUuid) {
        if (dataflowUuid == null || dataflowUuid.trim().isEmpty()) {
            return null;
        }
        String path = "/processors/list-sftp-processed-timestamp-by-dataflow?dataflowUuid=" + dataflowUuid;
        try {
            ResponseEntity<ProcessedTimestampResponse> response = client.get(path, false, ProcessedTimestampResponse.class);
            ProcessedTimestampResponse body = response != null ? response.getBody() : null;
            return body != null ? body.getProcessedTimestamp() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
