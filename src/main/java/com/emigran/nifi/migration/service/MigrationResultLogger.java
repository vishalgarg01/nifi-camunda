package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MigrationResultLogger {
    private static final Logger log = LoggerFactory.getLogger(MigrationResultLogger.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final File outputDir;
    private File outputFile;
    private final List<Map<String, Object>> succeeded = new ArrayList<>();
    private final List<Map<String, Object>> failed = new ArrayList<>();

    @Autowired
    public MigrationResultLogger(MigrationProperties properties) {
        this.outputDir = new File(properties.getLogDir());
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        initOutputFile();
    }

    private void initOutputFile() {
        this.outputFile = new File(outputDir, "nifi-migration-" + Instant.now().toEpochMilli() + ".jsonl");
    }

    /**
     * Clears succeeded/failed lists and creates a new log file. Call at the start of each migration run.
     */
    public synchronized void clear() {
        succeeded.clear();
        failed.clear();
        initOutputFile();
    }

    public synchronized void logSuccess(String workspaceName, String dataflowName, String message) {
        logSuccess(workspaceName, dataflowName, null, message);
    }

    public synchronized void logSuccess(String workspaceName, String dataflowName, String dataflowUuid, String message) {
        Map<String, Object> entry = baseEntry(workspaceName, dataflowName, dataflowUuid);
        entry.put("status", "success");
        entry.put("message", message);
        write(entry);
        succeeded.add(copyForSummary(entry));
    }

    public synchronized void logFailure(String workspaceName, String dataflowName, String message, Throwable ex) {
        logFailure(workspaceName, dataflowName, null, message, ex);
    }

    public synchronized void logFailure(String workspaceName, String dataflowName, String dataflowUuid, String message, Throwable ex) {
        Map<String, Object> entry = baseEntry(workspaceName, dataflowName, dataflowUuid);
        entry.put("status", "failure");
        entry.put("message", message);
        if (ex != null) {
            entry.put("error", ex.getMessage());
        }
        write(entry);
        failed.add(copyForSummary(entry));
    }

    private Map<String, Object> baseEntry(String workspaceName, String dataflowName, String dataflowUuid) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("ts", Instant.now().toString());
        entry.put("workspace", workspaceName);
        entry.put("dataflow", dataflowName);
        if (dataflowUuid != null) {
            entry.put("dataflowUuid", dataflowUuid);
        }
        return entry;
    }

    private Map<String, Object> copyForSummary(Map<String, Object> entry) {
        Map<String, Object> copy = new HashMap<>(entry);
        copy.remove("ts");
        return copy;
    }

    private void write(Map<String, Object> entry) {
        try (FileWriter writer = new FileWriter(outputFile, true)) {
            writer.write(objectMapper.writeValueAsString(entry));
            writer.write(System.lineSeparator());
        } catch (IOException e) {
            log.error("Unable to persist migration entry {}", entry, e);
        }
    }

    public String getOutputPath() {
        return outputFile.getAbsolutePath();
    }

    /** Returns true if any dataflow failed in this run. */
    public synchronized boolean hasFailures() {
        return !failed.isEmpty();
    }

    /**
     * Writes a summary JSON file with succeeded and failed dataflows, then returns its path.
     * File contains: { "succeeded": [...], "failed": [...], "logFile": "...", "summary": { "totalSucceeded", "totalFailed" } }
     */
    public synchronized String writeSummary() {
        Map<String, Object> summary = new HashMap<>();
        summary.put("succeeded", new ArrayList<>(succeeded));
        summary.put("failed", new ArrayList<>(failed));
        summary.put("logFile", getOutputPath());
        Map<String, Object> counts = new HashMap<>();
        counts.put("totalSucceeded", succeeded.size());
        counts.put("totalFailed", failed.size());
        summary.put("summary", counts);
        File summaryFile = new File(outputDir, "nifi-migration-summary-" + Instant.now().toEpochMilli() + ".json");
        try (FileWriter writer = new FileWriter(summaryFile)) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(writer, summary);
        } catch (IOException e) {
            log.error("Unable to write migration summary", e);
            return "";
        }
        log.info("Migration summary written to {}", summaryFile.getAbsolutePath());
        return summaryFile.getAbsolutePath();
    }
}
