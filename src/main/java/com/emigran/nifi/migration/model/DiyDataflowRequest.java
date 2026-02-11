package com.emigran.nifi.migration.model;

import java.util.List;

public class DiyDataflowRequest {
    private String name;
    private List<DiyBlockRequest> blockList;

    public DiyDataflowRequest() {
    }

    public DiyDataflowRequest(String name, List<DiyBlockRequest> blockList) {
        this.name = name;
        this.blockList = blockList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<DiyBlockRequest> getBlockList() {
        return blockList;
    }

    public void setBlockList(List<DiyBlockRequest> blockList) {
        this.blockList = blockList;
    }
}
