package com.emigran.nifi.migration.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from GET /api/processors/list-sftp-processed-timestamp-by-dataflow.
 * Contains the processed.timestamp from the first ListSFTPProcessor in the dataflow.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessedTimestampResponse {
    @JsonProperty("processedTimestamp")
    private String processedTimestamp;

    public String getProcessedTimestamp() {
        return processedTimestamp;
    }

    public void setProcessedTimestamp(String processedTimestamp) {
        this.processedTimestamp = processedTimestamp;
    }
}
