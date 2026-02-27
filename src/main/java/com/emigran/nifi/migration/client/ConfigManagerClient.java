package com.emigran.nifi.migration.client;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.emigran.nifi.migration.model.configmanager.ConfigAddResponse;
import com.emigran.nifi.migration.model.configmanager.ConfigResourceRequest;
import com.emigran.nifi.migration.model.configmanager.ConfigResourceResponse;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class ConfigManagerClient {
    private static final Logger log = LoggerFactory.getLogger(ConfigManagerClient.class);

    private final RestTemplate restTemplate = new RestTemplate();
    private final MigrationProperties properties;

    @Autowired
    public ConfigManagerClient(MigrationProperties properties) {
        this.properties = properties;
    }

    public List<ConfigResourceResponse> createConfigs(List<ConfigResourceRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            return Collections.emptyList();
        }
        String baseUrl = properties.getConfigManagerBaseUrl();
        String bearerToken = properties.getConfigManagerBearerToken();
        String orgId = properties.getConfigManagerOrgId();
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalStateException("config-manager base URL not configured");
        }
        if (bearerToken == null || bearerToken.isEmpty()) {
            throw new IllegalStateException("config-manager bearer token not configured");
        }
        if (orgId == null || orgId.isEmpty()) {
            throw new IllegalStateException("config-manager org id not configured");
        }

        for (ConfigResourceRequest r : requests) {
            if (r.getOrgId() == null || r.getOrgId().isEmpty()) {
                r.setOrgId(orgId);
            }
            if (r.getIsSecret() == null) {
                r.setIsSecret(Boolean.FALSE);
            }
        }

        String uri = baseUrl.endsWith("/") ? baseUrl + "configs/add" : baseUrl + "/configs/add";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setBearerAuth(bearerToken);
        headers.add("x-cap-api-auth-org-id", orgId);

        HttpEntity<List<ConfigResourceRequest>> entity = new HttpEntity<>(requests, headers);
        try {
            ResponseEntity<ConfigAddResponse> response = restTemplate.exchange(
                    uri,
                    HttpMethod.POST,
                    entity,
                    ConfigAddResponse.class);
            ConfigAddResponse body = response.getBody();
            if (body != null && Boolean.TRUE.equals(body.getSuccess())) {
                return body.getResult() != null ? body.getResult() : Collections.emptyList();
            }
            log.error("[ConfigManager] Add configs failed: {}", body);
            throw new RuntimeException("Config manager add failed");
        } catch (Exception ex) {
            log.error("[ConfigManager] Add configs failed: {}", ex.getMessage(), ex);
            throw ex;
        }
    }
}
