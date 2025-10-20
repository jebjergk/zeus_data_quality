# zeus_data_quality

Interim data quality solution for Zeus until a governance-native alternative is available.

## Technical overview

### Architecture
- **Streamlit-in-Snowflake (SiS)** hosts the primary UI (`streamlit_app.py`). The app runs with the current Snowflake session context and never issues `USE` statements; role, warehouse, and database selection stay inherited from Snowsight.
- **Snowpark** powers data processing inside stored procedures and Dynamic Table Function (DMF) evaluations. Snowpark sessions are bound to the Streamlit session’s context and respect the `EXECUTE AS CALLER` pattern to avoid privilege elevation surprises.
- **Dynamic Masking Functions (DMFs)** back the profiling and semantics engines by returning views of failing rows. Aggregate checks (`AGG:`) execute within Snowpark and push summarized results back into metadata tables instead of DMF outputs.
- **Tasks and stored procedures**: every saved configuration creates a `DQ_TASK_<CONFIG_ID>` task scheduled for 08:00 Europe/Berlin. Tasks call the `DQ_RUN_CONFIG(VARCHAR)` stored procedure, which orchestrates Snowpark jobs and writes outcomes to `DQ_RESULTS` tables.
- _Diagram placeholders_: `docs/diagrams/architecture.png` for the high-level platform flow and `docs/diagrams/data-movement.png` for task orchestration (to be supplied).

### Modules and services
- **Profiling** (`services/profiling`, `utils/profile_rules.py`): calculates column-level statistics, DMF validation rules, and anomaly thresholds.
- **Semantics** (`services/semantics`, `views/semantics`): defines table- and column-level business rules, including DMF-backed failed-row surfacing.
- **Scheduling** (`services/scheduler`, `sql/CREATE_RESULTS_AND_SP.SQL`): handles task creation, cron alignment, and stored procedure deployment.
- **Meta utilities** (`utils`, `tools`): shared helpers for Snowflake connectivity, logging, migration management, and snapshot mirroring.

### Developer platform and delivery flow
- Primary development happens through **ChatGPT (GPT-5 Thinking)** paired with the **Codex container** for local execution and testing.
- GitHub flow relies on short-lived feature branches targeting `dev_bs`. The “Auto PR & Squash Merge to dev_bs” workflow opens pull requests automatically, waits for checks, squashes, and deletes merged branches. Developers never push to `main` or `dev_bs` directly.
- Snapshot mirroring keeps `.py` sources accessible in restricted environments: `tools/mirror_snapshots.py` mirrors code into `/snapshots/*.p`, with `.github/workflows/mirror-snapshots.yml` committing updates on pushes to `dev_bs`.
- Auto-generated pull requests require concise titles and bullet-point summaries. All CI workflows must remain enabled to preserve the automation chain.

### Operational constraints and preflight checks
- Streamlit SQL must use positional `?` parameters with `params=[...]` (no named parameters) and omit `USE` statements to comply with Snowsight execution restrictions.
- Stored procedures execute with `EXECUTE AS CALLER`; every new procedure should be reviewed to ensure no inadvertent privilege escalation.
- Bootstrap scripts perform preflight checks for metadata tables (`DQ_CONFIG`, `DQ_CHECK`, `DQ_RESULTS`) and stored procedures. Run `sql/run_dq_config.sql` if preflight validation fails before enabling schedules.

## Source snapshots for copy/paste
To view code in environments where `.py` is blocked, this repo generates read-only mirrors under `/snapshots` with `.p` extension.

Run:
  python -m tools snapshot

Outputs:
  - snapshots/<same structure>.p
  - snapshots/SOURCE_SNAPSHOT.md

Do not edit files in `/snapshots`; they are generated from the real sources.
