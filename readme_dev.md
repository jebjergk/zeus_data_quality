# Zeus Data Quality — Dev Notes

- Entrypoint: `streamlit_app.py` (Streamlit-in-Snowflake).
- SQL params: use positional `?` with `params=[...]` (no named dict params).
- “DMFs” are **views of failing rows**; aggregate checks (`AGG:`) are **not attached**.
- Discovery via **INFORMATION_SCHEMA** (SHOW fallback). **No `RESULT_SCAN`**.
- Picker is **stateless**; editor persists one key: `editor_target_fqn`.
- Metadata tables: `DQ_CONFIG`, `DQ_CHECK` (created automatically).
- Daily task: `DQ_TASK_<CONFIG_ID>` at 08:00 Europe/Berlin.

## First run
- Open the app in Snowsight. The Configurations page will create tables on first save.
- If you see permission errors, check role/warehouse privileges.

## Troubleshooting task creation
- Creating a schedule requires the stored procedure `DQ_RUN_CONFIG(VARCHAR)` to be deployed in the metadata schema (the same database + schema that contain `DQ_CONFIG` and `DQ_CHECK`).
- If the Configurations page reports `Stored procedure DQ_RUN_CONFIG(VARCHAR) not found` or the task creation error `SQL compilation error: Object does not exist`, rerun the bootstrap script from [`sql/CREATE_RESULTS_AND_SP.SQL`](sql/CREATE_RESULTS_AND_SP.SQL) (or `sql/run_dq_config.sql`) in the desired schema to redeploy the stored procedure.
- After redeploying the stored procedure, retry enabling the schedule from the app. Tasks are created as `DQ_TASK_<CONFIG_ID>` in the metadata schema and require access to the target warehouse.
