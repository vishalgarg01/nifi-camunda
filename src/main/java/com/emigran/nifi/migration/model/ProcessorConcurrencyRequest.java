package com.emigran.nifi.migration.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Request body for PUT /api/processors/concurrency to update
 * concurrentlySchedulableTaskCount for a processor in a dataflow.
 */
@JsonInclude(Include.NON_NULL)
public class ProcessorConcurrencyRequest {
    private String neoDataflowId;   // neo dataflow id
    private String neoVersionId;    // neo version id
    private String blockType;
    private String blockId;
    private String blockName;
    private String processorType;
    private Integer concurrentlySchedulableTaskCount;

    public String getNeoDataflowId() {
        return neoDataflowId;
    }

    public void setNeoDataflowId(String neoDataflowId) {
        this.neoDataflowId = neoDataflowId;
    }

    public String getNeoVersionId() {
        return neoVersionId;
    }

    public void setNeoVersionId(String neoVersionId) {
        this.neoVersionId = neoVersionId;
    }

    public String getBlockType() {
        return blockType;
    }

    public void setBlockType(String blockType) {
        this.blockType = blockType;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getBlockName() {
        return blockName;
    }

    public void setBlockName(String blockName) {
        this.blockName = blockName;
    }

    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    public Integer getConcurrentlySchedulableTaskCount() {
        return concurrentlySchedulableTaskCount;
    }

    public void setConcurrentlySchedulableTaskCount(Integer concurrentlySchedulableTaskCount) {
        this.concurrentlySchedulableTaskCount = concurrentlySchedulableTaskCount;
    }
}
