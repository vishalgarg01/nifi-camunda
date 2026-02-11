package com.emigran.nifi.migration.model;

import java.util.List;

public class WorkspaceCreateRequest {
    private String name;
    private List<Organisation> organisations;

    public WorkspaceCreateRequest() {
    }

    public WorkspaceCreateRequest(String name, List<Organisation> organisations) {
        this.name = name;
        this.organisations = organisations;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Organisation> getOrganisations() {
        return organisations;
    }

    public void setOrganisations(List<Organisation> organisations) {
        this.organisations = organisations;
    }
}
