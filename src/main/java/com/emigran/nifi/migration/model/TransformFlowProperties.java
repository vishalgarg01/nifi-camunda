package com.emigran.nifi.migration.model;

/**
 * Properties extracted from flow.xml for a transform-type dataflow (HeaderProcessor,
 * GroupByWithLimit/AggregatorProcessor, JoltTransformJSON), used to generate JSLT
 * and configure convert_csv_to_json, jslt_transform, jolt_transform blocks.
 */
public class TransformFlowProperties {
    private String headerMappingJson;
    private String dateColumnOutputKey;
    private String existingDateFormat;
    private String newDateFormat;
    private String timezoneId;
    /** Record Group By from GroupByWithLimit (output/mapped names). Resolve to source names for convert_csv_to_json. */
    private String recordGroupBy;
    /** Minimum Group Record or Records Per Split (value < 100) for convert_csv_to_json groupSize. */
    private String groupSize;
    /** Sort Headers (comma-separated) for OptionalSortAndStreamProcessor. */
    private String sortHeaders;
    /** Use Alphabetical Sort (true/false). */
    private String alphabeticalSort;
    /** EvaluateJsonPath / attribution fields for convert_csv_to_json (e.g. $input.attributionType). */
    private String attributionType;
    private String attributionCode;
    private String headerValue;
    private String childTillCode;
    private String childOrgId;
    private String joltSpec;
    private String lineNo;

    public String getHeaderMappingJson() {
        return headerMappingJson;
    }

    public void setHeaderMappingJson(String headerMappingJson) {
        this.headerMappingJson = headerMappingJson;
    }

    public String getDateColumnOutputKey() {
        return dateColumnOutputKey;
    }

    public void setDateColumnOutputKey(String dateColumnOutputKey) {
        this.dateColumnOutputKey = dateColumnOutputKey;
    }

    public String getExistingDateFormat() {
        return existingDateFormat;
    }

    public void setExistingDateFormat(String existingDateFormat) {
        this.existingDateFormat = existingDateFormat;
    }

    public String getNewDateFormat() {
        return newDateFormat;
    }

    public void setNewDateFormat(String newDateFormat) {
        this.newDateFormat = newDateFormat;
    }

    public String getTimezoneId() {
        return timezoneId;
    }

    public void setTimezoneId(String timezoneId) {
        this.timezoneId = timezoneId;
    }

    public String getRecordGroupBy() {
        return recordGroupBy;
    }

    public void setRecordGroupBy(String recordGroupBy) {
        this.recordGroupBy = recordGroupBy;
    }

    public String getLineNo() {
        return lineNo;
    }

    public void setLineNo(String lineNo) {
        this.lineNo = lineNo;
    }

    public String getJoltSpec() {
        return joltSpec;
    }

    public void setJoltSpec(String joltSpec) {
        this.joltSpec = joltSpec;
    }

    public String getGroupSize() {
        return groupSize;
    }

    public void setGroupSize(String groupSize) {
        this.groupSize = groupSize;
    }

    public String getSortHeaders() {
        return sortHeaders;
    }

    public void setSortHeaders(String sortHeaders) {
        this.sortHeaders = sortHeaders;
    }

    public String getAlphabeticalSort() {
        return alphabeticalSort;
    }

    public void setAlphabeticalSort(String alphabeticalSort) {
        this.alphabeticalSort = alphabeticalSort;
    }

    public String getAttributionType() {
        return attributionType;
    }

    public void setAttributionType(String attributionType) {
        this.attributionType = attributionType;
    }

    public String getAttributionCode() {
        return attributionCode;
    }

    public void setAttributionCode(String attributionCode) {
        this.attributionCode = attributionCode;
    }

    public String getHeaderValue() {
        return headerValue;
    }

    public void setHeaderValue(String headerValue) {
        this.headerValue = headerValue;
    }

    public String getChildTillCode() {
        return childTillCode;
    }

    public void setChildTillCode(String childTillCode) {
        this.childTillCode = childTillCode;
    }

    public String getChildOrgId() {
        return childOrgId;
    }

    public void setChildOrgId(String childOrgId) {
        this.childOrgId = childOrgId;
    }
}
