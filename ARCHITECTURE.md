# Zeus Data Quality Architecture

This document provides a snapshot of the Zeus Data Quality Streamlit
application, focusing on the modules that define the UI, service layer, and
Snowflake integration points.

## High-level layout
- **`streamlit_app.py`** bootstraps Streamlit, handles navigation between pages,
  and wires together views with the service layer. It resolves Snowflake
  metadata namespaces and exposes helper functions such as `navigate_to` and
  `_get_page_from_query_params`.
- **`views/`** contains Streamlit page renderers. Each module exposes a
  `render_*` function that accepts the data it needs and writes directly to the
  Streamlit session.
  - `docs_view.py` renders in-app documentation using cached metadata.
  - `profile_view.py` shows profiling results, charts, and run history tables.
  - `table_picker.py` manages selection widgets and session cache tokens.
- **`services/`** houses business logic and Snowflake orchestration helpers. The
  modules are UI-agnostic and return plain data objects.
  - `configs.py` persists configurations and related checks.
  - `profiling_v2.py` calls `DQ_PROFILE_FULL` and retrieves profiling metadata.
  - `runner.py` launches data-quality tasks and polls results.
  - `state.py` stores Streamlit session state snapshots in Snowflake.
  - `semantics.py` maps domain concepts (checks, schedules) to Snowflake assets.
- **`utils/`** collects shared helpers.
  - `dmfs.py` establishes Snowflake Snowpark sessions, runs tasks, and mirrors
    metadata tables.
  - `meta.py` defines configuration dataclasses and metadata lookup functions.
  - `checkdefs.py`, `configs.py`, and `schedules.py` encapsulate validation
    rules, config parsing, and cron-like schedule helpers.
  - `ui.py` centralizes repeated Streamlit widgets and styling primitives.
- **`sql/`** and **`db/`** contain deployment artifacts. SQL scripts define the
  stored procedures, tasks, and tables required for the monitoring pipeline.
  The `db/` folder stores sample schemas for local development.

## Snowflake objects
The application relies on these key Snowflake assets:
- **Metadata tables** (`DQ_CONFIG`, `DQ_CHECK`, `DQ_RUN_RESULTS`) identified via
  `utils.configs.get_metadata_namespace`.
- **Stored procedures** (`RUN_DQ_CONFIG`) that execute data-quality checks.
- **Tasks** that orchestrate recurring runs (`run_task_now`, `task_name_for_config`).
- **Session state storage** handled through `services.state` utilities.

Ensure any schema or object changes remain backward compatible. Breaking
alterations require a coordinated migration plan and updated SQL scripts.

## Extension points
- **Adding a new page**: create a module in `views/`, register its key in
  `ALLOWED_PAGES` inside `streamlit_app.py`, and implement a renderer that calls
  the relevant services.
- **New business capability**: add a service in `services/` that encapsulates
  Snowflake calls, expose helper functions in `utils/` if needed, and keep the
  Streamlit layer thin.
- **Custom rules or validations**: extend `utils.checkdefs` or
  `utils.schedules`, ensuring new rules serialize cleanly to the Snowflake
  metadata tables.
- **Snowflake object changes**: update the SQL artifacts in `sql/`, document the
  rollout steps, and coordinate with database administrators.

### When in doubt, ask before changing UI/flows
The Streamlit navigation, sidebar layout, and workflow steps represent a
contract with data consumers. If a proposed change touches navigation or user
journeys, confirm requirements with product/design stakeholders first.
