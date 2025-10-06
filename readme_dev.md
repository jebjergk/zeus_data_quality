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
