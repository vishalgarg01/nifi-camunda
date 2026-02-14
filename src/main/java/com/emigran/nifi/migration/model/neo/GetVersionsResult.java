package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GetVersionsResult {
    private String refId;
    private String name;
    private Boolean active;
    private String liveVersion;
    private List<VersionInfo> versions;

    public String getRefId() {
        return refId;
    }

    public void setRefId(String refId) {
        this.refId = refId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public String getLiveVersion() {
        return liveVersion;
    }

    public void setLiveVersion(String liveVersion) {
        this.liveVersion = liveVersion;
    }

    public List<VersionInfo> getVersions() {
        return versions;
    }

    public void setVersions(List<VersionInfo> versions) {
        this.versions = versions;
    }
}
