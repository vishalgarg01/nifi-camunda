package com.emigran.workflow.delegates.nifi;

import com.emigran.nifi.migration.service.NifiMigrationService;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("nifiDataflowMigrationDelegate")
public class NifiDataflowMigrationDelegate implements JavaDelegate {
    private static final Logger log = LoggerFactory.getLogger(NifiDataflowMigrationDelegate.class);
    private final NifiMigrationService migrationService;

    @Autowired
    public NifiDataflowMigrationDelegate(NifiMigrationService migrationService) {
        this.migrationService = migrationService;
    }

    @Override
    public void execute(DelegateExecution execution) {
        log.info("Starting NiFi migration Camunda task");
        String logPath = migrationService.migrateAll();
        execution.setVariable("migrationLogPath", logPath);
        log.info("NiFi migration completed. Log at {}", logPath);
    }
}
