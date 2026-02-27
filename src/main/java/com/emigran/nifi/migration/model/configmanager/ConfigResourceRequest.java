package com.emigran.nifi.migration.model.configmanager;

public class ConfigResourceRequest {
    private String configName;
    private String configValue;
    private String orgId;
    private Boolean isSecret;

    public ConfigResourceRequest() {
    }

    public ConfigResourceRequest(String configName, String configValue, String orgId, Boolean isSecret) {
        this.configName = configName;
        this.configValue = configValue;
        this.orgId = orgId;
        this.isSecret = isSecret;
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getConfigValue() {
        return configValue;
    }

    public void setConfigValue(String configValue) {
        this.configValue = configValue;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public Boolean getIsSecret() {
        return isSecret;
    }

    public void setIsSecret(Boolean isSecret) {
        this.isSecret = isSecret;
    }
}
