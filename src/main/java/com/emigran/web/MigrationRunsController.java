package com.emigran.web;

import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.history.HistoricVariableInstance;
import org.camunda.bpm.engine.variable.Variables;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple UI and API to list migration process instances and start new ones.
 * Use the same app (port 8085) so you see instances from the engine that ran the job.
 */
@RestController
public class MigrationRunsController {

    private static final Logger log = LoggerFactory.getLogger(MigrationRunsController.class);
    private static final String PROCESS_KEY = "nifiMigrationProcess";
    private static final int MAX_RUNS = 50;

    private final RuntimeService runtimeService;
    private final HistoryService historyService;

    public MigrationRunsController(RuntimeService runtimeService, HistoryService historyService) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
    }

    /**
     * Simple HTML page: list of migration runs + form to start a new one.
     * Open http://localhost:8085/migration in the same app where the API triggered the job.
     */
    @GetMapping(value = "/migration", produces = MediaType.TEXT_HTML_VALUE)
    public String migrationPage() {
        return "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>NiFi Migration Runs</title>"
                + "<style>body{font-family:sans-serif;margin:20px;} table{border-collapse:collapse;margin-top:16px;} th,td{border:1px solid #ccc;padding:8px 12px;text-align:left;} th{background:#eee;} .running{color:#c00;} .success{color:#080;} .failure{color:#c00;} form{margin-top:20px;padding:12px;background:#f5f5f5;max-width:500px;} input,button{margin:4px;padding:6px;} button{cursor:pointer;}</style></head><body>"
                + "<h1>NiFi Migration Runs</h1><p>Process instances for <strong>nifiMigrationProcess</strong>. Same engine as REST API (this app).</p>"
                + "<div id=\"runs\">Loading...</div><h2>Start migration</h2>"
                + "<form id=\"startForm\"><label>Workspace ID (optional): <input name=\"workspaceId\" type=\"text\" placeholder=\"e.g. 493\"></label><br>"
                + "<label>Dataflow UUID (optional): <input name=\"dataflowId\" type=\"text\" placeholder=\"UUID\"></label><br><button type=\"submit\">Start migration</button></form>"
                + "<div id=\"startResult\" style=\"margin-top:8px;\"></div>"
                + "<script>function loadRuns(){fetch('/migration/runs').then(function(r){if(!r.ok)throw new Error(r.status+' '+r.statusText); return r.json();}).then(function(runs){"
                + "if(!Array.isArray(runs)){document.getElementById('runs').innerHTML='<p>Unexpected response (not a list).</p>'; return;} "
                + "if(runs.length===0){document.getElementById('runs').innerHTML='<p>No process instances yet. Start one below or via API.</p>';return;} "
                + "var html='<table><tr><th>Instance ID</th><th>State</th><th>Start</th><th>End</th><th>Log path</th><th>Summary path</th></tr>';"
                + "runs.forEach(function(r){var sc=r.state==='RUNNING'?'running':(r.hasFailures?'failure':'success'); var st=r.state==='RUNNING'?'Running':(r.hasFailures?'Failure':'Success'); "
                + "var lp=r.logPath?'<span title=\"'+r.logPath+'\">'+r.logPath+'</span>':'-'; var sp=r.summaryPath?'<span title=\"'+r.summaryPath+'\">'+r.summaryPath+'</span>':'-'; "
                + "html+='<tr><td>'+r.id+'</td><td class=\"'+sc+'\">'+st+'</td><td>'+(r.startTime||'-')+'</td><td>'+(r.endTime||'-')+'</td><td>'+lp+'</td><td>'+sp+'</td></tr>';}); "
                + "html+='</table>';document.getElementById('runs').innerHTML=html;}).catch(function(e){document.getElementById('runs').innerHTML='<p>Error loading runs: '+e.message+'</p>';});} "
                + "loadRuns();setInterval(loadRuns,10000); "
                + "document.getElementById('startForm').onsubmit=function(e){e.preventDefault();var fd=new FormData(this);var params=new URLSearchParams();if(fd.get('workspaceId'))params.set('workspaceId',fd.get('workspaceId'));if(fd.get('dataflowId'))params.set('dataflowId',fd.get('dataflowId')); "
                + "fetch('/migration/start?'+params,{method:'POST'}).then(function(r){if(!r.ok)throw new Error(r.status); return r.json();}).then(function(d){"
                + "var msg=d.instanceId?'Started: instance <strong>'+d.instanceId+'</strong>. Table refreshes in a few sec.':'Started. Instance: '+JSON.stringify(d);"
                + "document.getElementById('startResult').innerHTML=msg; loadRuns();}).catch(function(e){document.getElementById('startResult').innerHTML='Error: '+e.message;});};"
                + "</script></body></html>";
    }

    /**
     * List recent migration process instances (running + completed).
     * GET /migration/runs
     */
    @GetMapping(value = "/migration/runs", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<Map<String, Object>> listRuns() {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            List<HistoricProcessInstance> list = historyService.createHistoricProcessInstanceQuery()
                    .processDefinitionKey(PROCESS_KEY)
                    .orderByProcessInstanceStartTime()
                    .desc()
                    .maxResults(MAX_RUNS)
                    .list();

        for (HistoricProcessInstance pi : list) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", pi.getId());
            row.put("state", pi.getEndTime() == null ? "RUNNING" : "ENDED");
            row.put("startTime", pi.getStartTime() != null ? pi.getStartTime().toString() : null);
            row.put("endTime", pi.getEndTime() != null ? pi.getEndTime().toString() : null);
            try {
                Map<String, Object> vars = new HashMap<>();
                for (HistoricVariableInstance v : historyService.createHistoricVariableInstanceQuery()
                        .processInstanceId(pi.getId())
                        .list()) {
                    vars.put(v.getVariableName(), v.getValue());
                }
                row.put("logPath", vars.get("migrationLogPath"));
                row.put("summaryPath", vars.get("migrationSummaryPath"));
                Object hasFailures = vars.get("migrationHasFailures");
                row.put("hasFailures", Boolean.TRUE.equals(hasFailures));
            } catch (Exception e) {
                row.put("logPath", null);
                row.put("summaryPath", null);
                row.put("hasFailures", false);
            }
            result.add(row);
        }
        } catch (Exception e) {
            log.warn("Failed to list migration runs: {}", e.getMessage());
        }
        return result;
    }

    /**
     * Start a new migration process instance.
     * POST /migration/start?workspaceId=493&dataflowId=uuid
     */
    @PostMapping(value = "/migration/start", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> startMigration(
            @RequestParam(required = false) String workspaceId,
            @RequestParam(required = false) String dataflowId) {
        Map<String, Object> variables = new HashMap<>();
        if (workspaceId != null && !workspaceId.isBlank()) {
            variables.put("workspaceId", Variables.stringValue(workspaceId.trim()));
        }
        if (dataflowId != null && !dataflowId.isBlank()) {
            variables.put("dataflowId", Variables.stringValue(dataflowId.trim()));
        }
        var instance = runtimeService.startProcessInstanceByKey(PROCESS_KEY, variables);
        Map<String, Object> out = new HashMap<>();
        out.put("instanceId", instance.getId());
        out.put("processDefinitionId", instance.getProcessDefinitionId());
        return out;
    }
}
