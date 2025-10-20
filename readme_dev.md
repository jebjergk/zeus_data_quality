# Zeus Data Quality — Developer Notes

This reference keeps implementation details crisp so engineers can support the Streamlit application and Snowflake assets without digging through code.

## Core entry points
- **Streamlit app**: `streamlit_app.py` (Streamlit in Snowflake deployment).
- **Configuration storage**: `DQ_CONFIG`, `DQ_CHECK`, and `DQ_RUN_RESULTS` metadata tables.
- **Runner procedure**: `DQ_RUN_CONFIG(VARCHAR)` orchestrates DMF and aggregate checks.

> **Tip:** The table picker remains stateless. Persist the selected table in `editor_target_fqn` when reopening the editor.

## First run checklist
1. Open the app in Snowsight. Saving the first configuration creates metadata tables automatically.
2. Confirm the active role and warehouse can create tasks and execute stored procedures in the metadata schema.
3. If permissions fail, rerun [`sql/CREATE_RESULTS_AND_SP.SQL`](sql/CREATE_RESULTS_AND_SP.SQL) or `sql/run_dq_config.sql`.

## Working with SQL from Streamlit
- Always use positional placeholders (`?`) with `params=[...]`; named parameters are blocked in Snowsight Streamlit.
- DMF artefacts are views of failing rows. Aggregate checks (prefixed `AGG:`) never materialise DMF views.
- Discovery pulls from `INFORMATION_SCHEMA` (with `SHOW` as a fallback) and avoids `RESULT_SCAN` usage.

## Scheduling behaviour
- Each configuration maps to `DQ_TASK_<CONFIG_ID>` scheduled for 08:00 Europe/Berlin.
- Tasks call the stored procedure with `EXECUTE AS CALLER`, inheriting the user’s context and warehouse.
- Toggle enablement directly in Streamlit; the app creates, resumes, or suspends the Snowflake task as needed.

## Troubleshooting task creation
- Missing procedure errors (`Object does not exist`) point to the runner procedure being absent from the metadata schema.
- After redeploying the procedure, retry schedule creation so the task can bind to the target warehouse.
- Review `DQ_RUN_RESULTS` for the latest execution message if a run fails after scheduling.
