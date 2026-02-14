package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateRuleResponse {
    private Boolean success;
    private Integer status;
    private CreateRuleResult result;

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

    public CreateRuleResult getResult() {
        return result;
    }

    public void setResult(CreateRuleResult result) {
        this.result = result;
    }
}
