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

