package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.model.RuleMeta;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.Collections;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

/**
 * Loads RulesMetas_cps.json and provides block type definitions (config properties, defaults).
 */
@Service
public class RulesMetasService {
    private static final Logger log = LoggerFactory.getLogger(RulesMetasService.class);
    private static final String RULES_METAS_PATH = "RulesMetas_cps.json";

    private final ObjectMapper objectMapper = new ObjectMapper();
    /** Block type id (e.g. "sftp_read") -> property name -> default value or null */
    private Map<String, Map<String, Object>> blockTypeToProperties = Collections.emptyMap();
    /** Block type id -> property name -> full property schema (includes uiSchema/useSecret/etc.) */
    private Map<String, Map<String, RuleMeta.PropertySchema>> blockTypeToPropertySchemas = Collections.emptyMap();

    @PostConstruct
    public void load() {
        try {
            ClassPathResource resource = new ClassPathResource(RULES_METAS_PATH);
            try (InputStream is = resource.getInputStream()) {
                RuleMeta[] array = objectMapper.readValue(is, RuleMeta[].class);
                Map<String, Map<String, Object>> map = new LinkedHashMap<>();
                Map<String, Map<String, RuleMeta.PropertySchema>> schemaMap = new LinkedHashMap<>();
                for (RuleMeta meta : array) {
                    String id = meta.getId();
                    if (id == null) continue;
                    Map<String, Object> props = new LinkedHashMap<>();
                    Map<String, RuleMeta.PropertySchema> propSchemas = new LinkedHashMap<>();
                    if (meta.getConfig() != null && meta.getConfig().getProperties() != null) {
                        for (Map.Entry<String, RuleMeta.PropertySchema> e : meta.getConfig().getProperties().entrySet()) {
                            Object def = e.getValue().getDefaultValue();
                            props.put(e.getKey(), def);
                            propSchemas.put(e.getKey(), e.getValue());
                        }
                    }
                    map.put(id, props);
                    schemaMap.put(id, propSchemas);
                }
                this.blockTypeToProperties = map;
                this.blockTypeToPropertySchemas = schemaMap;
                log.info("[RulesMetas] Loaded {} block types from {}", map.size(), RULES_METAS_PATH);
            }
        } catch (Exception e) {
            log.error("[RulesMetas] Failed to load {}: {}", RULES_METAS_PATH, e.getMessage());
        }
    }

    /**
     * Returns the config property keys and their default values for the given block type (e.g. "sftp_read").
     * If the block type is not found, returns empty map.
     */
    public Map<String, Object> getConfigDefaultsForBlockType(String blockTypeId) {
        Map<String, Object> props = blockTypeToProperties.get(blockTypeId);
        if (props == null) return Collections.emptyMap();
        return new LinkedHashMap<>(props);
    }

    /**
     * Returns the config property schemas for the given block type (full schema from RulesMetas).
     */
    public Map<String, RuleMeta.PropertySchema> getPropertySchemasForBlockType(String blockTypeId) {
        Map<String, RuleMeta.PropertySchema> props = blockTypeToPropertySchemas.get(blockTypeId);
        if (props == null) return Collections.emptyMap();
        return new LinkedHashMap<>(props);
    }

    /**
     * Returns true if the block type is defined in RulesMetas.
     */
    public boolean hasBlockType(String blockTypeId) {
        return blockTypeToProperties.containsKey(blockTypeId);
    }

    /**
     * All known block type ids.
     */
    public List<String> getBlockTypeIds() {
        return new ArrayList<>(blockTypeToProperties.keySet());
    }
}
