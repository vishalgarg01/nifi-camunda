package com.emigran.nifi.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BlockDefinitionResponse {

    private List<UiField> uiFields;
    private List<NiFiProcessorDefinition> processors;

    public List<UiField> getUiFields() {
        return uiFields;
    }

    public void setUiFields(List<UiField> uiFields) {
        this.uiFields = uiFields;
    }

    public List<NiFiProcessorDefinition> getProcessors() {
        return processors;
    }

    public void setProcessors(List<NiFiProcessorDefinition> processors) {
        this.processors = processors;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UiField {
        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NiFiProcessorDefinition {
        private Long id;
        private List<NiFiPropertyMapping> properties;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public List<NiFiPropertyMapping> getProperties() {
            return properties;
        }

        public void setProperties(List<NiFiPropertyMapping> properties) {
            this.properties = properties;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NiFiPropertyMapping {
        private Long fieldId;
        private String nifiKey;

        public Long getFieldId() {
            return fieldId;
        }

        public void setFieldId(Long fieldId) {
            this.fieldId = fieldId;
        }

        public String getNifiKey() {
            return nifiKey;
        }

        public void setNifiKey(String nifiKey) {
            this.nifiKey = nifiKey;
        }
    }
}
