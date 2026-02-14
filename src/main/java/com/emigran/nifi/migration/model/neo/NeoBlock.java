package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class NeoBlock {
    private String name;
    private Map<String, Object> config;
    private Map<String, String> variableConfigKeyMap;
    private BlockPosition position;
    private List<BlockRelation> relations;
    private String type;
    private boolean source;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    public Map<String, String> getVariableConfigKeyMap() {
        return variableConfigKeyMap;
    }

    public void setVariableConfigKeyMap(Map<String, String> variableConfigKeyMap) {
        this.variableConfigKeyMap = variableConfigKeyMap;
    }

    public BlockPosition getPosition() {
        return position;
    }

    public void setPosition(BlockPosition position) {
        this.position = position;
    }

    public List<BlockRelation> getRelations() {
        return relations;
    }

    public void setRelations(List<BlockRelation> relations) {
        this.relations = relations;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isSource() {
        return source;
    }

    public void setSource(boolean source) {
        this.source = source;
    }
}
