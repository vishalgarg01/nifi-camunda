package com.emigran.nifi.migration.client;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.emigran.nifi.migration.model.BlockDefinitionResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Component
public class BlockDefinitionClient {

    private final RestTemplate restTemplate;
    private final MigrationProperties properties;

    @Autowired
    public BlockDefinitionClient(MigrationProperties properties) {
        this.restTemplate = new RestTemplate();
        this.properties = properties;
    }

    public BlockDefinitionResponse getBlockDefinition(long blockTypeId) {
        String url = properties.getGlueBlockDefinitionUrl().replace("{blockTypeId}", String.valueOf(blockTypeId));
        String expanded = UriComponentsBuilder.fromHttpUrl(url).build(true).toUriString();
        HttpHeaders headers = new HttpHeaders();
        String token = properties.getBasicAuthToken();
        if (token != null && !token.isEmpty()) {
            headers.add(HttpHeaders.AUTHORIZATION, "Basic " + token);
        }
        HttpEntity<Void> entity = new HttpEntity<>(headers);
        ResponseEntity<BlockDefinitionResponse> response = restTemplate.exchange(
                expanded, HttpMethod.GET, entity, BlockDefinitionResponse.class);
        return response.getBody();
    }
}
