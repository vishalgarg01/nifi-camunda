package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BlockRelation {
    private String name;
    private String expression;
    private List<String> status;
    private String to;

    public BlockRelation() {}

    public BlockRelation(String name, String expression, List<String> status, String to) {
        this.name = name;
        this.expression = expression;
        this.status = status;
        this.to = to;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public List<String> getStatus() {
        return status;
    }

    public void setStatus(List<String> status) {
        this.status = status;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
