package com.emigran.nifi.migration.model.neo;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateRuleRequest {
    private String name;
    private List<String> tags;
    private String applicationId;
    private String context;
    private List<String> usersForReportingEmail;

    public CreateRuleRequest() {}

    public CreateRuleRequest(String name, List<String> tags, String applicationId, String context, List<String> usersForReportingEmail) {
        this.name = name;
        this.tags = tags;
        this.applicationId = applicationId;
        this.context = context;
        this.usersForReportingEmail = usersForReportingEmail;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }
    public List<String> getUsersForReportingEmail() {
        return usersForReportingEmail;
    }

    public void setUsersForReportingEmail(List<String> usersForReportingEmail) {
        this.usersForReportingEmail = usersForReportingEmail;
    }
}
