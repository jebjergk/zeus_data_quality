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

## Data governance for EU and German stakeholders

### Regulatory alignment made simple
- **GDPR**: Customer datasets stay inside Snowflake’s EU region. We only persist configuration metadata, run logs, and profiling statistics—never the underlying personal data values. Metadata is scoped to the minimum necessary fields, fulfilling data minimisation and purpose limitation duties.
- **BaFin circulars & supervisory expectations**: Governance controls (roles, approvals, audit trail) remain embedded in the Snowflake security model so financial institutions can evidence proportional safeguards without re-platforming.
- **MiFID II record-keeping**: Historical task runs, check definitions, and remediation notes are retained to demonstrate monitoring of data feeding regulated reporting.
- **EU AI Act (draft principles)**: Optional AI-assisted features observe transparency and human-in-the-loop principles, working exclusively on metadata and retaining prompt/response fingerprints for traceability.

### What the platform stores—and what it avoids
- **Configurations**: Check definitions, scheduling metadata, and Snowflake object references only.
- **Task and run results**: Status flags, row counts, exception summaries, timestamps, and operator notes. Profiling and AI helper features log statistics (e.g., min/max, distinct counts) but never surface raw values.
- **No PII export**: Profiling outputs, semantic checks, and AI recommendations rely on metadata. Sample rows or customer-identifying values never leave the secure Snowflake tenancy.
- **EU-only processing**: All computation and storage sit in the customer’s Snowflake EU account; the Streamlit front-end reuses the active Snowsight session without rerouting traffic abroad.

### Access control and operational safety nets
- **Roles-first access**: Streamlit executes `EXECUTE AS CALLER`, inheriting the signed-in user’s Snowflake role. Only authorised roles can view configurations or results; sensitive views remain masked downstream.
- **Role-gated advanced features**: AI-assisted prompts can be toggled per configuration and are visible only to users with the `DQ_AI_REVIEWER` role (or the customer-defined equivalent).
- **Audit trail clarity**: Every run records the triggering task, execution timestamps, Snowflake role, and outcome. When AI assistance is enabled, the prompt hash, anonymised prompt payload, and model identifier are stored alongside the run metadata for traceability.

### Data quality dimensions and enforcing checks
- **Completeness**: Null/blank detection rules and row-count comparisons ensure required attributes are populated.
- **Accuracy**: Range validations, referential checks, and semantic rules compare metadata against trusted reference sources.
- **Consistency**: Cross-table reconciliations and format validations catch mismatches between related datasets.
- **Timeliness**: Task schedules monitor late-arriving data by comparing expected and actual refresh timestamps.
- **Uniqueness**: Key integrity checks flag duplicate business identifiers or unexpected cardinality changes.

Each dimension maps to explicit check templates stored in the configuration metadata and executed within Snowpark; failed checks land in metadata tables for remediation without exposing raw customer data.

### Customer data protection messaging
- **No raw data to language models**: Optional AI helpers summarise profiling metadata only. Raw customer data never enters model prompts or responses.
- **Metadata-only insights**: Statistical summaries, schema drift indicators, and anomaly scores drive recommendations while keeping PII shielded.
- **Feature toggles for comfort**: Organisations can disable AI helpers globally or per configuration; defaults favour manual review in regulated environments.
- **Transparent operator experience**: Users see when AI assistance is active, which model provided guidance, and can review prompt hashes before accepting suggestions.

This governance layer gives compliance, risk, and business teams a shared language for understanding how Zeus Data Quality protects customer information while maintaining the rigor expected by EU and German regulators.
