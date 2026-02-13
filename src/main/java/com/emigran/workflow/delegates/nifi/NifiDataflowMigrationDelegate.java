package com.emigran.workflow.delegates.nifi;

import com.emigran.nifi.migration.service.MigrationResultLogger;
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
    private final MigrationResultLogger resultLogger;

    @Autowired
    public NifiDataflowMigrationDelegate(NifiMigrationService migrationService,
                                        MigrationResultLogger resultLogger) {
        this.migrationService = migrationService;
        this.resultLogger = resultLogger;
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
        String summaryPath = resultLogger.writeSummary();
        boolean hasFailures = resultLogger.hasFailures();
        execution.setVariable("migrationLogPath", logPath);
        execution.setVariable("migrationSummaryPath", summaryPath != null ? summaryPath : "");
        execution.setVariable("migrationHasFailures", hasFailures);
        log.info("[NifiDataflowMigrationDelegate] execute END - log={}, summary={}, hasFailures={}", logPath, summaryPath, hasFailures);
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
