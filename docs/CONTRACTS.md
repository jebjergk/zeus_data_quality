# Zeus DQ App — Data & Function Contracts

This file defines stable structures that code must adhere to.

---

## Profiling Output Contract

`run_table_profile()` must return:

(
summary: {
rows_profiled: int,
sample_pct: float | None,
},
columns: List[ColumnProfilePayload]
)

shell
Copy code

### ColumnProfilePayload Shape

{
column_name: str,
data_type: str,
nulls: int,
null_pct: float,
distincts: int,
distinct_pct: float,
min_val: any,
max_val: any,
avg_len: float,
whitespace_pct: float,
top_values: List[{ value: any, count: int, pct: float }],
semantic_type: str | None,
confidence: float | None, # 0-100 scale
rationale: str | None
}

markdown
Copy code

**Important:**  
- `""`, `" "` and `NULL` must be **distinct values** in `top_values`.
- `avg_len` must be computed via `LENGTH(CAST(col AS STRING))`.

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

yaml
Copy code

---

## DQ Execution Procedure Contract

`CALL DQ_RUN_CONFIG(config_id)` must:

- Read rows from `DQ_CHECK WHERE config_id = ?`
- Write results to `DQ_RUN_RESULTS`
- Return text string: `"OK run_id=<uuid> checks=<count>"`
