package com.emigran.nifi.migration.client;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.emigran.nifi.migration.model.neo.*;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Client for the Neo Rule API (canvas dataflow create, get versions, update version, post-hook).
 * Uses Cookie and x-cap-remote-user headers; base URL and credentials are from config.
 */
@Component
public class NeoRuleApiClient {
    private static final Logger log = LoggerFactory.getLogger(NeoRuleApiClient.class);

    private final RestTemplate restTemplate = new RestTemplate();
    private final MigrationProperties properties;

    @Autowired
    public NeoRuleApiClient(MigrationProperties properties) {
        this.properties = properties;
    }

    /**
     * Create an empty canvas dataflow. POST /rule/create?time=...
     *
     * @param dataflowName name (same as old dataflow)
     * @return the created rule result id (_id), or null if creation failed
     */
    public String createCanvasDataflow(String dataflowName) {
        String baseUrl = properties.getNeoRuleBaseUrl();
        if (baseUrl == null || baseUrl.isEmpty()) {
            log.warn("[NeoRuleApi] neo-rule base URL not configured; skipping create");
            return null;
        }
        String path = "/rule/create";
        long time = System.currentTimeMillis();
        String uri = UriComponentsBuilder.fromHttpUrl(baseUrl + path)
                .queryParam("time", time)
                .build(true)
                .toUriString();

        CreateRuleRequest body = new CreateRuleRequest(
                dataflowName,
                Collections.singletonList("migration"),
                properties.getNeoRuleApplicationId(),
                properties.getNeoRuleContext());

        HttpEntity<CreateRuleRequest> entity = new HttpEntity<>(body, buildHeaders());
        try {
            ResponseEntity<CreateRuleResponse> response = restTemplate.exchange(
                    uri, HttpMethod.POST, entity, CreateRuleResponse.class);
            CreateRuleResponse resp = response.getBody();
            if (resp != null && Boolean.TRUE.equals(resp.getSuccess()) && resp.getResult() != null) {
                String id = resp.getResult().getId();
                log.info("[NeoRuleApi] Created canvas dataflow {} -> id={}", dataflowName, id);
                return id;
            }
            log.warn("[NeoRuleApi] Create response missing or not success: {}", resp);
            return null;
        } catch (Exception ex) {
            log.error("[NeoRuleApi] Create failed: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * Get versions for a dataflow. GET /rule/{dataflowId}/versions?ruleType=org&context=...
     *
     * @param dataflowId _id from create response
     * @return first version's _id (DRAFT), or null
     */
    public String getFirstVersionId(String dataflowId) {
        String baseUrl = properties.getNeoRuleBaseUrl();
        if (baseUrl == null || baseUrl.isEmpty()) {
            return null;
        }
        String path = "/rule/" + dataflowId + "/versions";
        String uri = UriComponentsBuilder.fromHttpUrl(baseUrl + path)
                .queryParam("ruleType", "org")
                .queryParam("context", properties.getNeoRuleContext())
                .build(true)
                .toUriString();

        HttpEntity<Void> entity = new HttpEntity<>(buildHeaders());
        try {
            ResponseEntity<GetVersionsResponse> response = restTemplate.exchange(
                    uri, HttpMethod.GET, entity, GetVersionsResponse.class);
            GetVersionsResponse resp = response.getBody();
            if (resp != null && Boolean.TRUE.equals(resp.getSuccess()) && resp.getResult() != null
                    && resp.getResult().getVersions() != null && !resp.getResult().getVersions().isEmpty()) {
                VersionInfo first = resp.getResult().getVersions().get(0);
                String versionId = first.getId();
                log.info("[NeoRuleApi] Got version id={} for dataflow {}", versionId, dataflowId);
                return versionId;
            }
            log.warn("[NeoRuleApi] Get versions response missing or empty: {}", resp);
            return null;
        } catch (Exception ex) {
            log.error("[NeoRuleApi] Get versions failed: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * Update version with blocks, schedule, tag. POST /rule/{dataflowId}/version/{versionId}/update?context=...
     */
    public void updateVersion(String dataflowId, String versionId, VersionUpdateRequest request) {
        String baseUrl = properties.getNeoRuleBaseUrl();
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalStateException("neo-rule base URL not configured");
        }
        String path = "/rule/" + dataflowId + "/version/" + versionId + "/update";
        String uri = UriComponentsBuilder.fromHttpUrl(baseUrl + path)
                .queryParam("context", properties.getNeoRuleContext())
                .build(true)
                .toUriString();

        HttpEntity<VersionUpdateRequest> entity = new HttpEntity<>(request, buildHeaders());
        try {
            restTemplate.exchange(uri, HttpMethod.POST, entity, Void.class);
            log.info("[NeoRuleApi] Updated version {} for dataflow {}", versionId, dataflowId);
        } catch (Exception ex) {
            log.error("[NeoRuleApi] Update version failed: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * Post-hook to make dataflow live. POST /rule/{dataflowId}/version/{versionId}/post-hook-for-connect-plus
     */
    public void postHookForConnectPlus(String dataflowId, String versionId) {
        String baseUrl = properties.getNeoRuleBaseUrl();
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalStateException("neo-rule base URL not configured");
        }
        String path = "/rule/" + dataflowId + "/version/" + versionId + "/post-hook-for-connect-plus";
        String uri = baseUrl + path;

        HttpEntity<Void> entity = new HttpEntity<>(buildHeaders());
        try {
            ResponseEntity<PostHookResponse> response = restTemplate.exchange(
                    uri, HttpMethod.POST, entity, PostHookResponse.class);
            PostHookResponse resp = response.getBody();
            if (resp != null && Boolean.TRUE.equals(resp.getSuccess())) {
                log.info("[NeoRuleApi] Post-hook success for dataflow {}", dataflowId);
            } else {
                log.warn("[NeoRuleApi] Post-hook response: {}", resp);
            }
        } catch (Exception ex) {
            log.error("[NeoRuleApi] Post-hook failed: {}", ex.getMessage(), ex);
            throw ex;
        }
    }

    private HttpHeaders buildHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        String cookie = properties.getNeoRuleCookie();
        if (cookie != null && !cookie.isEmpty()) {
            headers.add(HttpHeaders.COOKIE, cookie);
        }
        String remoteUser = properties.getNeoRuleRemoteUser();
        if (remoteUser != null && !remoteUser.isEmpty()) {
            headers.add("x-cap-remote-user", remoteUser);
        }
        return headers;
    }
}
