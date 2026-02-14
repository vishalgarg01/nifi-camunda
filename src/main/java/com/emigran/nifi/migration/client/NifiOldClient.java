package com.emigran.nifi.migration.client;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.emigran.nifi.migration.model.DataflowDetail;
import com.emigran.nifi.migration.model.DataflowSummary;
import com.emigran.nifi.migration.model.Workspace;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class NifiOldClient {

    private final NifiApiClient client;

    @Autowired
    public NifiOldClient(MigrationProperties properties) {
        RestTemplate restTemplate = new RestTemplate();
        this.client = new NifiApiClient(restTemplate, properties.getOldBaseUrl(), properties.getBasicAuthToken(),
                properties.getSourceHeader());
    }

    public List<Workspace> getWorkspaces() {
        ResponseEntity<Workspace[]> response = client.get("/workspaces", false, Workspace[].class);
        Workspace[] body = response.getBody();
        return body == null ? Collections.emptyList() : Arrays.asList(body);
    }

    /**
     * Fetch a single workspace by id. Use when user provides workspaceId to avoid listing all workspaces.
     */
    public Workspace getWorkspace(String workspaceId) {
        if (workspaceId == null || workspaceId.trim().isEmpty()) {
            return null;
        }
        ResponseEntity<Workspace> response = client.get("/workspaces/" + workspaceId.trim(), false, Workspace.class);
        return response.getBody();
    }

    public List<DataflowSummary> getDataflows(Long workspaceId) {
        ResponseEntity<DataflowSummary[]> response = client.get("/workspaces/" + workspaceId + "/dataflows", false,
                DataflowSummary[].class);
        DataflowSummary[] body = response.getBody();
        return body == null ? Collections.emptyList() : Arrays.asList(body);
    }

    public DataflowDetail getDataflowDetail(Long workspaceId, String uuid) {
        ResponseEntity<DataflowDetail> response = client.get(
                "/workspaces/" + workspaceId + "/dataflows/" + uuid, false, DataflowDetail.class);
        return response.getBody();
    }
}
