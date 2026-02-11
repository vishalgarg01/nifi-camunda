package com.emigran.nifi.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataflowSummary {
    private String name;
    private String uuid;
    private DataflowStatus status;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public DataflowStatus getStatus() {
        return status;
    }

    public void setStatus(DataflowStatus status) {
        this.status = status;
    }
}
