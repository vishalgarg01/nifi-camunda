package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class VersionUpdateRequest {
    private List<NeoBlock> blocks;
    private String schedule;
    private String tag;

    public List<NeoBlock> getBlocks() {
        return blocks;
    }

    public void setBlocks(List<NeoBlock> blocks) {
        this.blocks = blocks;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
