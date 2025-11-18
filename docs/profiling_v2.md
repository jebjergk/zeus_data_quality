# Profiling v2 quick reference

Profiling v2 is the only profiling experience in the app. The UI simply orchestrates a deterministic Snowflake workflow and then renders the metadata it produces. Use this page as the hand-off guide when digging into behaviour or debugging runs.

## End-to-end pipeline

```
UI action ➜ CALL DQ_PROFILE_FULL(:IN_TABLE_FQN)
           ➜ DQ_SAVE_PROFILE_RESULTS
           ➜ DQ_CLASSIFY_COLUMNS_HEURISTIC
           ➜ DQ_APPLY_RULES
```

* `DQ_PROFILE_FULL` captures summary metrics and logs the run. The Streamlit app calls `services.profiling_v2.run_profiling_v2` which wraps this stored procedure.
* `DQ_SAVE_PROFILE_RESULTS` persists column-level features into tables exposed to the UI.
* `DQ_CLASSIFY_COLUMNS_HEURISTIC` performs semantic tagging and sets the confidence/source metadata shown in the column grid.
* `DQ_APPLY_RULES` writes recommended checks (the suggestions drawer) based on the tags and metrics.

All procedures live in the `DISCOVERY` schema (`profiling_v2.DISCOVERY_NAMESPACE`) and run as `EXECUTE AS CALLER`, so caller roles must have access to both the metadata schema and the source table being profiled.

## Tables the UI reads

| Object | Purpose |
| --- | --- |
| `DQ_PROFILE_RUN` | Run history, status banner, duration, and troubleshooting details. |
| `DQ_TABLE_PROFILE_SUMMARY` | High-level stats (rows profiled, sampling choice, duration) for the hero metrics. |
| `DQ_COLUMN_FEATURES` | Column metrics such as null %, distinct %, min/max, and sample size. |
| `DQ_COLUMN_CLASSIFICATION` | Semantic tags, source (manual vs heuristic), and confidence. |
| `DQ_SUGGESTED_CHECKS` | Recommended checks that can seed the configuration editor. |

These tables are always referenced with the namespace defined by `services.profiling_v2.DISCOVERY_NAMESPACE` (defaults to `ZEUS_ANALYTICS_SIMU.DISCOVERY`).

## Manual verification in Snowflake

1. Choose a fully qualified table name (e.g. `DEMO_DB.PUBLIC.CUSTOMERS`).
2. Ensure your role can `USAGE` the database/schema and `SELECT` the table plus the `DISCOVERY` schema objects.
3. Run the profiling procedure directly:
   ```sql
   CALL ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_FULL('DEMO_DB.PUBLIC.CUSTOMERS');
   ```
4. Inspect the latest run to confirm status:
   ```sql
   SELECT *
     FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN
    WHERE TARGET_TABLE = 'DEMO_DB.PUBLIC.CUSTOMERS'
 ORDER BY STARTED_AT DESC
    LIMIT 5;
   ```
5. Review saved metrics and tags:
   ```sql
   SELECT COLUMN_NAME, NULL_RATIO, DISTINCT_RATIO
     FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_FEATURES
    WHERE TARGET_TABLE = 'DEMO_DB.PUBLIC.CUSTOMERS'
 ORDER BY COLUMN_NAME;
   ```
6. Fetch semantic classifications or suggested checks if you need to compare against the UI grids.

## Additional notes

* Procedures are lightweight and retry-safe; the UI simply increments `profile_data_nonce` to force reloads after each call.
* Metadata refreshes are idempotent—the UI always re-reads the tables listed above instead of caching Snowflake data locally.
* Suggested checks intentionally prefer WARN severities unless a heuristic has >0.9 confidence.
* Profiling jobs can be run repeatedly; the most recent `RUN_ID` recorded in `DQ_PROFILE_RUN` is used to show status in the Streamlit view.
