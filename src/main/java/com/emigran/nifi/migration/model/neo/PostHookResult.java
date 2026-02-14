package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostHookResult {
    private String message;
    private Boolean isSuccess;
    private String dataflowName;
    private String neoDataflowId;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Boolean getIsSuccess() {
        return isSuccess;
    }

    public void setIsSuccess(Boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public String getDataflowName() {
        return dataflowName;
    }

    public void setDataflowName(String dataflowName) {
        this.dataflowName = dataflowName;
    }

    public String getNeoDataflowId() {
        return neoDataflowId;
    }

    public void setNeoDataflowId(String neoDataflowId) {
        this.neoDataflowId = neoDataflowId;
    }
}
