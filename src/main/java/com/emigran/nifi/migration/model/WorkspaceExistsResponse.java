package com.emigran.nifi.migration.model;

public class WorkspaceExistsResponse {
    private String name;
    private boolean exists;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }
}
