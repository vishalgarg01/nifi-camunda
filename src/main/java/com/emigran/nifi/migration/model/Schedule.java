package com.emigran.nifi.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Schedule {
    private String cron;
    private Long cronFrequencyInSeconds;
    private boolean enabled;
    private String lastExecutionTimestamp;
    private String scheduleStartTime;
    private String scheduleStopTime;
    private String scheduleExpression;

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public Long getCronFrequencyInSeconds() {
        return cronFrequencyInSeconds;
    }

    public void setCronFrequencyInSeconds(Long cronFrequencyInSeconds) {
        this.cronFrequencyInSeconds = cronFrequencyInSeconds;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getLastExecutionTimestamp() {
        return lastExecutionTimestamp;
    }

    public void setLastExecutionTimestamp(String lastExecutionTimestamp) {
        this.lastExecutionTimestamp = lastExecutionTimestamp;
    }

    public String getScheduleStartTime() {
        return scheduleStartTime;
    }

    public void setScheduleStartTime(String scheduleStartTime) {
        this.scheduleStartTime = scheduleStartTime;
    }

    public String getScheduleStopTime() {
        return scheduleStopTime;
    }

    public void setScheduleStopTime(String scheduleStopTime) {
        this.scheduleStopTime = scheduleStopTime;
    }

    public String getScheduleExpression() {
        return scheduleExpression;
    }

    public void setScheduleExpression(String scheduleExpression) {
        this.scheduleExpression = scheduleExpression;
    }
}
