# Zeus DQ App — Development Guardrails

These rules must be respected in all PRs.  
They exist to prevent regressions and UI instability.

---

## UI Guardrails

| Area | Rule |
|---|---|
| Sidebar Layout | Do **not** add or remove sidebar pages without explicit request. |
| Profile Result Grid | The **Include** checkbox must remain **inside the main profile grid**, not in a separate grid. |
| Top Values Section | Must always display NULL, empty string "", and whitespace cases distinctly. |
| Confidence Display | Must remain color-coded: Green ≥ 90%, Yellow ≥ 75%, Gray below. |
| Column Order | Do not reorder profile result columns unless requested. |

---

## Profiling Guardrails

1. **Never trim values** during profiling → empty vs whitespace vs null must remain distinguishable.
2. **Min/Max for TEXT must show actual values**, truncated at 50 chars, not lengths.
3. **Avg Length must work for both TEXT & NUMBER fields** (convert number to string for length calculation).
4. **Date detection must be evidence-based** (pattern & parse success), never name-based.

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
