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
        log.info("[NifiDataflowMigrationDelegate] execute START");
        String workspaceId = getStringVariable(execution, "workspaceId");
        String dataflowId = getStringVariable(execution, "dataflowId");
        if (workspaceId != null || dataflowId != null) {
            log.info("[NifiDataflowMigrationDelegate] Migration scope: workspaceId={}, dataflowId={}", workspaceId, dataflowId);
        } else {
            log.info("[NifiDataflowMigrationDelegate] Migration scope: all workspaces and dataflows");
        }
        log.info("[NifiDataflowMigrationDelegate] Calling migrationService.migrateAll");
        String logPath = migrationService.migrateAll(workspaceId, dataflowId);
        execution.setVariable("migrationLogPath", logPath);
        log.info("[NifiDataflowMigrationDelegate] execute END - result log at {}", logPath);
    }

    private static String getStringVariable(DelegateExecution execution, String name) {
        Object value = execution.getVariable(name);
        if (value == null) {
            return null;
        }
        String s = value.toString().trim();
        return s.isEmpty() ? null : s;
    }
}
