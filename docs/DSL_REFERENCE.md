# DSL Reference

This page summarizes the DSL used for data quality rules. Use it alongside the in-app editor for quick reminders and copyable snippets.

## Macros

- `:col` → Column value expression (uses alias `T`).
- `:col_name` → String literal of the column name (useful for lookup patterns).
- `:param_name` → Parameters defined in the schema (for example `:min_value`, `:pattern`).
- `:table` → Table macro when available in your DSL context.

## Supported constructs (with examples)

- **Null checks**
  - `:col IS NULL`
  - `:col IS NOT NULL`
- **Regex**
  - `REGEXP_LIKE(:col, :pattern)`
- **Range**
  - `:col BETWEEN :min_value AND :max_value`
- **IN list**
  - `:col IN (:allowed_values)`
- **Exists lookup**
  - `EXISTS (SELECT 1 FROM :ref_table R WHERE R.:ref_value_col = :col)`
  - `EXISTS (SELECT 1 FROM :ref_table R WHERE R.:ref_name_col = :col_name AND R.:ref_value_col = :col)`
- **String helpers**
  - `TRIM(:col)`, `UPPER(:col)`, `LOWER(:col)`, `LENGTH(:col)`
- **Other helpers**
  - Combine predicates with `AND` / `OR`
  - Negate with `NOT (<predicate>)`

## Common mistakes to avoid

- Snowflake uses `IS NULL` / `IS NOT NULL` (not `ISNULL()`).
- Quote literals, not macros. Leave macros such as `:col` and `:param_name` unquoted.
- Table aliases: expressions expect column references to use alias `T` if needed.
- Ensure parameter names in the DSL match the parameter schema exactly.

