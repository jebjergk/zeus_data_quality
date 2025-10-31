UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Controls (top to bottom):
1. Header row with two columns sized `[1, 8]`.  Left column must contain the "⬅ Back" button wired to return to the list view.  Right column must render the header "Edit Configuration" when editing or "Create Configuration" when creating.
2. When profile suggestions are applied, show a single success alert summarising rows profiled and sample percentage exactly as implemented; no additional banners precede the Target section.
3. Subheader "Target" with the stateless table picker (database, schema, table) capturing the selected fully qualified name into `editor_target_fqn`.  Immediately below, display the caption `Target Table: <value>` where `<value>` resolves to the selected table or "— not selected —".
4. Heading "### Columns" followed by a multiselect labelled "Columns to check" listing available table columns.  The informational message "Table-level checks **FRESHNESS** and **ROW_COUNT_ANOMALY** are automatically included." must follow directly beneath the multiselect.
5. Configuration form `cfg_form` containing elements in this fixed order:
   a. Subheader "Configuration".
   b. Disabled text input "Name" with automatic derivation help text.  No manual name entry control may be added.
   c. Text area "Description" allowing optional free-form notes.
   d. For each selected column, render an expander titled `Column: <column>` (collapsed by default).  Inside each expander, controls must appear exactly as follows:
      • Number input "Sample failing rows for <column>" (0–1000, default 10).
      • Checkbox "UNIQUE".  When checked, show the `Ignore NULLs` checkbox (defaults True) followed by the selectbox `Severity (UNIQUE)` with options `ERROR`, `WARN` in that order.
      • Checkbox "NULL_COUNT".  When checked, show number input `Max NULL rows` (minimum 0) then selectbox `Severity (NULL_COUNT)` with options `ERROR`, `WARN`.
      • Checkbox "MIN_MAX".  When checked, show text inputs `Min (inclusive)` and `Max (inclusive)` (both default empty strings) followed by selectbox `Severity (MIN_MAX)` with options `ERROR`, `WARN`.
      • Checkbox "WHITESPACE".  When checked, show selectbox `Mode` with the three options `NO_LEADING_TRAILING`, `NO_INTERNAL_ONLY_WHITESPACE`, `NON_EMPTY_TRIMMED` in that exact order, followed by selectbox `Severity (WHITESPACE)` with options `ERROR`, `WARN`.
      • Checkbox "FORMAT_DISTRIBUTION".  When checked, show text input `Regex (Snowflake RLIKE)`, number input `Min match ratio (0-1)` (range 0.0–1.0, step 0.01), then selectbox `Severity (FORMAT_DISTRIBUTION)` with options `ERROR`, `WARN`.
      • Checkbox "VALUE_DISTRIBUTION".  When checked, show text input `Allowed values (CSV)`, number input `Min in-set ratio (0-1)` (range 0.0–1.0, step 0.01), then selectbox `Severity (VALUE_DISTRIBUTION)` with options `ERROR`, `WARN`.
   e. Heading "### Table-level checks (always included)" with the text input `Timestamp column for table checks`, caption explaining failure behaviour, and number input `Freshness max age (minutes)` (range 1–10080, step 30).  The values must synchronise with session state as in code.
   f. `st.form_submit_button` labelled "Preview last 60 days row counts" of type `secondary`.
   g. Heading "### Schedule" followed by checkbox `Enable daily task` (with explanatory help text), text input `Cron expression`, and text input `Timezone`.  The cron and timezone inputs are disabled when scheduling is unchecked.
   h. Final row of four submit buttons laid out in columns `[1,1,1,1]`: `Save & Apply` (primary by default), `Save as Draft`, `Run Now`, and `Delete` (secondary styling).
6. When preview is requested and prerequisites are satisfied, render a dataframe showing the columns `day` and `cnt` in that order with `use_container_width=True`, `hide_index=True`, and `height=320`.  No charts or alternative layouts are permitted.
7. Post-submit handling must reuse the existing success, warning, and info messaging pattern; no extra notifications precede or replace them.

Forbidden patterns:
• Do not reorder sections or controls listed above.
• Do not introduce additional check types, severity dropdowns, or per-column widgets beyond those specified.
• Do not add new scheduling controls, task toggles, or preview visualisations.
• Do not allow manual editing of the configuration name or target caption formatting.
