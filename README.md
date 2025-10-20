# Zeus Data Quality

Zeus Data Quality brings monitoring, profiling, and remediation workflows directly into Snowflake so data owners can spot and resolve issues before they affect reporting.

## Product snapshot
- **Use case**: Centralise data quality rules for business-critical tables without exporting data.
- **Audience**: Data stewards, analytics teams, and governance leads who need audit-ready oversight.
- **Delivery**: Streamlit in Snowflake front end, Snowpark stored procedures, and scheduled Snowflake tasks.

> **Note:** The Streamlit application inherits the active Snowsight session. Role, warehouse, and database context stay with the signed-in user.

## Version history

| Version | Highlights |
|---------|------------|
| v1.3    | Added profiling capabilities and semantic recommendations.
| v1.2    | Improved usability flows and hardened scheduling defaults.
| v1.1    | Simplified configuration and task orchestration.
| v1.0    | Initial release.

## Platform overview

### Application architecture
- **Streamlit in Snowflake** hosts the UI (`streamlit_app.py`) and runs inside the customer Snowflake account.
- **Snowpark** executes stored procedures and Dynamic Monitoring Framework (DMF) checks in-database, respecting caller context.
- **DMF views** expose failing rows for each rule, while aggregate checks summarise results in metadata tables.
- **Scheduled tasks** (`DQ_TASK_<CONFIG_ID>`) call `DQ_RUN_CONFIG(VARCHAR)` each morning at 08:00 Europe/Berlin.

### Modules and services
- **Profiling** (`services/profiling`, `utils/profile_rules.py`): Computes statistics, semantic tags, and recommendations.
- **Semantics** (`services/semantics`, `views/semantics`): Manages business rules and DMF-backed failure surfacing.
- **Scheduling** (`services/scheduler`, `sql/CREATE_RESULTS_AND_SP.SQL`): Creates tasks and deploys stored procedures.
- **Shared utilities** (`utils`, `tools`): Cover Snowflake connectivity, logging, migrations, and snapshot mirroring.

### Delivery guardrails
- Develop on short-lived branches that target `dev_bs`; rely on the "Auto PR & Squash Merge to dev_bs" workflow for merges.
- Keep snapshot mirroring enabled via `tools/mirror_snapshots.py` and `.github/workflows/mirror-snapshots.yml`.
- Maintain concise pull request titles with bullet summaries; do not disable existing CI workflows.

## Operational guidance

### Preflight checklist
- Use positional `?` parameters with `params=[...]` for all Streamlit SQL execution.
- Confirm stored procedures run with `EXECUTE AS CALLER` to keep privileges aligned with Snowflake roles.
- Run the bootstrap scripts (`sql/run_dq_config.sql`) if metadata tables (`DQ_CONFIG`, `DQ_CHECK`, `DQ_RESULTS`) are missing.

### Working with source snapshots
To support environments that block `.py` files, the repository mirrors all Python sources into `/snapshots` with `.p` extensions.

```
python -m tools snapshot
```

Outputs include mirrored files under `/snapshots` and a generated `snapshots/SOURCE_SNAPSHOT.md`. Do not edit snapshot files directly; rerun the mirror script instead.

## Data governance commitments

### Regulatory alignment
- **GDPR**: All processing occurs in the Snowflake EU region; only metadata, logs, and profiling statistics persist.
- **BaFin**: Role-based approvals and audit trails stay within the Snowflake security model for supervised institutions.
- **MiFID II**: Historical tasks, check definitions, and remediation notes remain available for regulatory evidence.
- **EU AI Act (draft)**: Optional AI-assisted features focus on metadata, retain prompt fingerprints, and enforce human oversight.

### What is stored
- **Configurations**: Object references, scheduling metadata, and rule definitions.
- **Run results**: Status values, row counts, summaries, timestamps, and operator notes.
- **Profiling outputs**: Aggregated statistics only—no raw personal data.

### Access and safety controls
- Streamlit executes with the caller's role; access to configurations and results follows Snowflake grants.
- AI-assisted prompts are role-gated (for example, `DQ_AI_REVIEWER`) and default to manual review.
- Every run logs its triggering task, role, timing, and procedure outcome for traceability.

### Quality dimensions
- **Completeness**: Null detection and row-count comparisons.
- **Accuracy**: Range, referential, and semantic checks.
- **Consistency**: Cross-table and format validations.
- **Timeliness**: Freshness checks against timestamp columns.
- **Uniqueness**: Duplicate detection for key identifiers.

### Customer data protections
- AI helpers use metadata only; raw values never enter model prompts.
- Statistical insights, schema drift indicators, and anomaly scores drive recommendations while keeping PII protected.
- Feature toggles allow organisations to disable AI helpers globally or per configuration.
- Operators see which model assisted, review prompt hashes, and approve guidance before it becomes active.

Zeus Data Quality provides a shared foundation for compliance, risk, and business teams to monitor critical Snowflake assets without moving data out of the platform.
