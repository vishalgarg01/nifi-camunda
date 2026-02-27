package com.emigran.nifi.migration.model.configmanager;

import java.util.List;

public class ConfigAddResponse {
    private Boolean success;
    private Integer status;
    private List<ConfigResourceResponse> result;

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

    public List<ConfigResourceResponse> getResult() {
        return result;
    }

    public void setResult(List<ConfigResourceResponse> result) {
        this.result = result;
    }
}
