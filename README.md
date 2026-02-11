## Camunda NiFi Test Project

Lightweight Spring Boot + Camunda project that wires up the `NifiDataflowMigrationDelegate` so you can exercise NiFi migration logic in isolation.

### How to run
- From this folder run `mvn spring-boot:run` (uses H2 and Camunda web apps on port `8085`).
- Camunda Tasklist/Cockpit/REST: http://localhost:8085/camunda/app and http://localhost:8085/engine-rest

### Trigger the delegate
- Start the test process: `POST http://localhost:8085/engine-rest/process-definition/key/nifiMigrationTest/start`
- The process consists of a single service task that calls the `nifiDataflowMigrationDelegate`. The delegate sets `migrationLogPath` in process variables.

### Configuration (application.yaml)
- Camunda admin user: `demo` / `demo`
- H2 in-memory database with schema auto-update.
- NiFi migration properties mirror the main app; override via env vars:
  - `NIFI_MIGRATION_OLD_BASE_URL` / `NIFI_MIGRATION_NEW_BASE_URL`
  - `NIFI_MIGRATION_AUTH_BASIC` (base64 basic token)
  - `NIFI_MIGRATION_SOURCE_HEADER`
  - `NIFI_MIGRATION_FLOW_XML_SFTP_*` and `NIFI_MIGRATION_FLOW_XML_SENSITIVE_KEY`
  - `NIFI_MIGRATION_GLUE_BLOCK_DEFINITION_URL`
  - `NIFI_MIGRATION_LOG_DIR`
  - `NIFI_MIGRATION_STRING_ENCRYPTOR_*`

### Notes
- The full NiFi migration client/service stack from `emigran` is copied here unchanged so behavior matches production logic (SFTP download + NiFi decrypt + REST clients).
- Default URLs point to localhost; provide real endpoints/secrets before running against actual systems.
