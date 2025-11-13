# Zeus Data Quality App — Architecture Overview

This document describes the structure, responsibilities, and data flow of the Zeus Data Quality (DQ) App.

## High-Level Purpose

The application provides:
- **Data profiling** on arbitrary tables to understand content, patterns, and quality risks.
- **Data quality rule configuration** (row-level + aggregate checks).
- **Automated task scheduling** for recurring execution.
- **Result logging and monitoring**.

The app is designed to run **fully inside Snowflake** using:
- Snowpark Python Stored Procedures
- Snowflake Tasks & Warehouses
- Snowflake Tables for metadata + logging
- Streamlit in Snowflake for UI

No data leaves Snowflake. No third-party AI inference is called.

---

## Code Layout

| Folder | Description |
|-------|-------------|
| `streamlit_app.py` | Main UI router, controls active page -->
| `views/` | UI tabs (profile, configs, docs, etc.) |
| `services/` | Business logic modules and Snowflake interactions |
| `utils/` | Low-level helpers (FQN parsing, DMF attachment, metadata lookup) |

---

## Data Flow (Profiling)

User selects table →
`services.profiling_v2.run_full_profile()` calls `DQ_PROFILE_FULL('<DB>.<SCHEMA>.<TABLE>')` →
Stored procedure saves metrics into `ZEUS_ANALYTICS_SIMU.DISCOVERY` tables →
`views.profile_view` queries `DQ_TABLE_PROFILE_SUMMARY`, `DQ_COLUMN_FEATURES`, `DQ_COLUMN_CLASSIFICATION`, and `DQ_SUGGESTED_CHECKS` →
Streamlit renders summaries, semantic tags, and suggested checks

yaml
Copy code

Profiling yields:
- Null counts & percentages
- Distinct counts & cardinality signals
- Min / max *actual values* for text + numeric fields
- Estimated semantic type (Identifier, REF Code, Contact, etc.)
- Confidence score with rationale

---

## Data Flow (DQ Execution)

User configures rule set → Stored in DQ_CHECK
Run Now or Scheduled Task → Executes DQ_RUN_CONFIG procedure →
Each check determines OK / FAIL →
Results written to DQ_RUN_RESULTS →
Displayed in UI

yaml
Copy code

Row-level checks use `WHERE NOT (predicate)` fail-count evaluation.  
Aggregate checks use `AGG:` prefixed expressions evaluating Boolean status.

---

## Stored Procedures

| Procedure | Purpose |
|----------|---------|
| `DQ_RUN_CONFIG(config_id)` | Execute all checks for a configuration and record results |
| `SP_DQ_MANAGE_TASK(...)` | Create / replace Snowflake Task for scheduled DQ runs |

Both run as **EXECUTE AS CALLER**, meaning:
- Execution inherits the **user’s role & warehouse**.
- No surprises with hidden owner-role privileges.

---

## Metadata Tables

| Table | Purpose |
|------|---------|
| `DQ_CHECK` | Definition of each DQ rule in a config |
| `DQ_RUN_RESULTS` | Execution output for each check run |
| `ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_TABLE_PROFILE_SUMMARY` | Table-level profiling snapshots |
| `ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_FEATURES` | Column statistics and signals |
| `ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_CLASSIFICATION` | Semantic tags per column |
| `ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_SUGGESTED_CHECKS` | Recommended DQ checks inferred from profiling |

---

## Architectural Principles

1. **All processing stays in Snowflake** (Privacy / GDPR safe).
2. **Profiling is read-only** — no mutation of data.
3. **UI state is stateless**, except selected table + config editing state.
4. **Profiling must not distort data** (empty string ≠ NULL).
5. **Semantic typing is guidance, never authoritative**.
