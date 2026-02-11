package com.emigran.nifi.migration.client;

import java.net.URI;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class NifiApiClient {
    private static final Logger log = LoggerFactory.getLogger(NifiApiClient.class);

    private final RestTemplate restTemplate;
    private final String baseUrl;
    private final String basicAuthToken;
    private final String sourceHeaderValue;

    public NifiApiClient(RestTemplate restTemplate, String baseUrl, String basicAuthToken, String sourceHeaderValue) {
        this.restTemplate = restTemplate;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.basicAuthToken = basicAuthToken;
        this.sourceHeaderValue = sourceHeaderValue;
    }

    public <T> ResponseEntity<T> get(String path, boolean includeMigrationHeader, Class<T> responseType) {
        HttpHeaders headers = buildBaseHeaders(includeMigrationHeader);
        return exchange(path, HttpMethod.GET, headers, null, responseType);
    }

    public <T> ResponseEntity<T> post(String path, Object body, boolean includeMigrationHeader, Class<T> responseType) {
        HttpHeaders headers = buildBaseHeaders(includeMigrationHeader);
        return exchange(path, HttpMethod.POST, headers, body, responseType);
    }

    public <T> ResponseEntity<T> put(String path, Object body, boolean includeMigrationHeader, Class<T> responseType) {
        HttpHeaders headers = buildBaseHeaders(includeMigrationHeader);
        return exchange(path, HttpMethod.PUT, headers, body, responseType);
    }

    private HttpHeaders buildBaseHeaders(boolean includeMigrationHeader) {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (basicAuthToken != null && !basicAuthToken.isEmpty()) {
            headers.add(HttpHeaders.AUTHORIZATION, "Basic " + basicAuthToken);
        }
        if (includeMigrationHeader && sourceHeaderValue != null && !sourceHeaderValue.isEmpty()) {
            headers.add("X-CAP-SOURCE", sourceHeaderValue);
        }
        return headers;
    }

    private <T> ResponseEntity<T> exchange(String path, HttpMethod method, HttpHeaders headers, Object body, Class<T> responseType) {
        URI uri = UriComponentsBuilder.fromHttpUrl(baseUrl + path).build(true).toUri();
        HttpEntity<Object> entity = new HttpEntity<>(body, headers);
        try {
            return restTemplate.exchange(uri, method, entity, responseType);
        } catch (Exception ex) {
            log.error("HTTP {} {} failed", method, uri, ex);
            throw ex;
        }
    }
}
