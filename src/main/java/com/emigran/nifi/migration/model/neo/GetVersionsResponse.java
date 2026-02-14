package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetVersionsResponse {
    private Boolean success;
    private Integer status;
    private GetVersionsResult result;

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public GetVersionsResult getResult() {
        return result;
    }

    public void setResult(GetVersionsResult result) {
        this.result = result;
    }
}
