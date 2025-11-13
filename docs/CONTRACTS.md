# Zeus DQ App — Data & Function Contracts

This file defines stable structures that code must adhere to.

---

## Profiling Metadata Contract

Profiling v2 no longer streams arbitrary payloads from Streamlit. The contract is:

1. **Execution** — `services.profiling_v2.run_full_profile(session, table_fqn)` must call `CALL ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_FULL('<DB>.<SCHEMA>.<TABLE>')`.
2. **Storage targets** — the stored procedure writes to `ZEUS_ANALYTICS_SIMU.DISCOVERY` tables: `DQ_TABLE_PROFILE_SUMMARY`, `DQ_COLUMN_FEATURES`, `DQ_COLUMN_CLASSIFICATION`, `DQ_SUGGESTED_CHECKS`, and `DQ_PROFILE_RUN`.
3. **Read-only UI** — Streamlit pages only `SELECT` from the metadata tables above. No direct scans of user tables are permitted inside the UI.
4. **Value fidelity** — ratio columns stay as decimals (0–1), min/max fields retain the actual text or numeric values, and timestamps are rendered without truncation.

---

## Suggested DQ Config Contract

The suggestion engine must produce:

{
"target_table": "DB.SCHEMA.TABLE",
"columns": [
{
"name": "COLUMN_NAME",
"include": true|false,
"recommended_checks": ["NOT_NULL", "WHITESPACE", ...],
"justification": "why"
}
]
}

---

## DQ Execution Procedure Contract

`CALL DQ_RUN_CONFIG(config_id)` must:

- Read rows from `DQ_CHECK WHERE config_id = ?`
- Write results to `DQ_RUN_RESULTS`
- Return text string: `"OK run_id=<uuid> checks=<count>"`
