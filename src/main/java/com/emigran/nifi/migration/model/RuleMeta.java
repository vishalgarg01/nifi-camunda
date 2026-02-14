package com.emigran.nifi.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/**
 * One entry from RulesMetas_cps.json: block type definition with config schema.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleMeta {
    private String id;
    private RuleMetaConfig config;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public RuleMetaConfig getConfig() {
        return config;
    }

    public void setConfig(RuleMetaConfig config) {
        this.config = config;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RuleMetaConfig {
        private Map<String, PropertySchema> properties;

        public Map<String, PropertySchema> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, PropertySchema> properties) {
            this.properties = properties;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PropertySchema {
        private String type;
        private Object defaultValue;
        private Object oneOf;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @com.fasterxml.jackson.annotation.JsonProperty("default")
        public Object getDefaultValue() {
            return defaultValue;
        }

        @com.fasterxml.jackson.annotation.JsonProperty("default")
        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }

        public Object getOneOf() {
            return oneOf;
        }

        public void setOneOf(Object oneOf) {
            this.oneOf = oneOf;
        }
    }
}
