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
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Component
public class NifiNewClient {

    private static final Logger log = LoggerFactory.getLogger(NifiNewClient.class);

    private final NifiApiClient client;
    private final MigrationProperties properties;
    private final RestTemplate restTemplate;

    @Autowired
    public NifiNewClient(MigrationProperties properties) {
        this.restTemplate = new RestTemplate();
        this.client = new NifiApiClient(restTemplate, properties.getNewBaseUrl(), properties.getBasicAuthToken(),
                properties.getSourceHeader());
        this.properties = properties;
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
     * Fetches the configured report recipients for a dataflow.
     * GET /dataflows/neo/recipients?dataflowId={dataflowId}
     *
     * @param dataflowId id of the dataflow on the new system
     * @return list of recipient email addresses (deduped/trimmed), empty if none or on error
     */
    public List<String> getDataflowReportRecipients(String dataflowId) {
        if (dataflowId == null || dataflowId.trim().isEmpty()) {
            return Collections.emptyList();
        }

        String baseUrl = properties.getNewBaseUrl();
        String orgId = properties.getConfigManagerOrgId();
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalStateException("new base URL not configured");
        }
        if (orgId == null || orgId.isEmpty()) {
            throw new IllegalStateException("config-manager org id not configured");
        }

        String uri = UriComponentsBuilder.fromHttpUrl(baseUrl + "/dataflows/neo/recipients")
                .queryParam("dataflowId", dataflowId.trim())
                .build(true)
                .toUriString();

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("x-cap-api-auth-org-id", orgId);
        if (properties.getBasicAuthToken() != null && !properties.getBasicAuthToken().isEmpty()) {
            headers.add(HttpHeaders.AUTHORIZATION, "Basic " + properties.getBasicAuthToken());
        }
        if (properties.getSourceHeader() != null && !properties.getSourceHeader().isEmpty()) {
            headers.add("X-CAP-SOURCE", properties.getSourceHeader());
        }

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        try {
            ResponseEntity<String[]> response = restTemplate.exchange(
                    uri, HttpMethod.GET, entity, String[].class);
            String[] body = response.getBody();
            if (body == null || body.length == 0) {
                return Collections.emptyList();
            }
            return Arrays.stream(body)
                    .map(email -> email != null ? email.trim() : null)
                    .filter(email -> email != null && !email.isEmpty())
                    .distinct()
                    .collect(Collectors.toList());
        } catch (Exception ex) {
            log.warn("[NifiNewClient] Failed to fetch report recipients for dataflow {}: {}", dataflowId, ex.getMessage());
            return Collections.emptyList();
        }
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
