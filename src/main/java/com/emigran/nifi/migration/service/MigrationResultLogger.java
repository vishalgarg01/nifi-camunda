package com.emigran.nifi.migration.service;

import com.emigran.nifi.migration.config.MigrationProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MigrationResultLogger {
    private static final Logger log = LoggerFactory.getLogger(MigrationResultLogger.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final File outputFile;

    @Autowired
    public MigrationResultLogger(MigrationProperties properties) {
        File dir = new File(properties.getLogDir());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        this.outputFile = new File(dir, "nifi-migration-" + Instant.now().toEpochMilli() + ".jsonl");
    }

    public synchronized void logSuccess(String workspaceName, String dataflowName, String message) {
        Map<String, Object> entry = baseEntry(workspaceName, dataflowName);
        entry.put("status", "success");
        entry.put("message", message);
        write(entry);
    }

    public synchronized void logFailure(String workspaceName, String dataflowName, String message, Throwable ex) {
        Map<String, Object> entry = baseEntry(workspaceName, dataflowName);
        entry.put("status", "failure");
        entry.put("message", message);
        if (ex != null) {
            entry.put("error", ex.getMessage());
        }
        write(entry);
    }

    private Map<String, Object> baseEntry(String workspaceName, String dataflowName) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("ts", Instant.now().toString());
        entry.put("workspace", workspaceName);
        entry.put("dataflow", dataflowName);
        return entry;
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
}
