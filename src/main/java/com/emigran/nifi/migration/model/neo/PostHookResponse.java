package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PostHookResponse {
    private Boolean success;
    private Integer status;
    private PostHookResult result;

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

    public PostHookResult getResult() {
        return result;
    }

    public void setResult(PostHookResult result) {
        this.result = result;
    }
}
