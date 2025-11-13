# Zeus DQ App — Development Guardrails

These rules must be respected in all PRs.  
They exist to prevent regressions and UI instability.

---

## UI Guardrails

| Area | Rule |
|---|---|
| Sidebar Layout | Do **not** add or remove sidebar pages without explicit request. |
| Profiling view | Keep the header + caption + metadata note, stateless picker, run + refresh buttons, summary metrics, and the four tabs (column features, semantic tags, suggested checks, run history). |
| Profiling data | All values displayed must come from `ZEUS_ANALYTICS_SIMU.DISCOVERY` metadata tables—no direct scans in the UI. |
| Debug state | Debug expanders stay behind the `DEBUG_PROFILING` flag only. |

---

## Profiling Guardrails

1. **Always call** `CALL ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_FULL('<FQN>')` instead of bespoke SQL.
2. **Read-only metadata** — load results from `DQ_TABLE_PROFILE_SUMMARY`, `DQ_COLUMN_FEATURES`, `DQ_COLUMN_CLASSIFICATION`, and `DQ_SUGGESTED_CHECKS`.
3. **Do not mutate metadata** from Streamlit; profiling procedures own inserts/updates.
4. **Respect value fidelity** when rendering ratios and min/max values (no trimming, no silent rounding beyond formatting for display).

---

## Semantic Type Guardrails

### Use Hybrid Label Style (Decision: C)

| Category | Label Format | Example Output |
|---|---|---|
| Identifier | `Identifier (ID)` | ACCOUNT_ID → "Identifier (ID)" |
| Reference Code | `Reference Code (REF)` | COUNTRY_CODE → "Reference Code (REF)" |
| Contact | `Contact (EMAIL)` | CUSTOMER_EMAIL → "Contact (EMAIL)" |
| Date Text | `Date (Text)` | ORDER_DATE_TEXT → "Date (Text)" |

**Never** force IBAN, BIC, ISIN, currency, etc. unless regex + length + checksum signals strongly match.

---

## Task & Stored Procedure Guardrails

- All stored procedures must be defined as:  
  `EXECUTE AS CALLER`
- Tasks must always specify warehouse explicitly.
- No automatic task execution on config save — only on **Run Now** or schedule.

---

## General Safety Rules

- Never drop tables or views.
- Never rewrite metadata tables without explicit request.
- Never call external LLMs with data — profiling must remain *local* and deterministic.
