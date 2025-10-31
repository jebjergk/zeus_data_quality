UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Controls (top to bottom):
1. Page header labelled "🧪 Profile Table" immediately followed by the caption "Profile a table to explore null rates, distinct counts, ranges, and common values before defining data quality checks."  The header and caption must remain paired with no intervening widgets.
2. Stateless table picker with three selectors (database, schema, table) sourced from `stateless_table_picker`.  It must appear directly under the caption and persist the selected fully qualified name in `st.session_state["profile_target_fqn"]`.
3. A horizontal divider separating the picker from the run controls.
4. Control row rendered as three equally spaced columns:
   • Column 1 contains the number input labelled "Sample %" (range 0–100, step 1).  Help text must include the metadata rationale and the instruction "Enter 0 for a full table scan."  The widget key stays `profile_sample_pct`.
   • Column 2 contains the number input labelled "Top N values" (range 1–10, default 10, step 1) with helper text "Collect up to 10 of the most common values per column."  No additional controls share this column.
   • Column 3 contains a selectbox labelled "Load saved profile" followed by a "Load" button.  The selectbox must always render, using "— Select a saved run —" when runs exist or the disabled option "— No saved profiles —" otherwise.  The Load button lives directly under the selectbox and is disabled unless a run is chosen and metadata targets are configured.  Caption messaging under the button must cover the Snowflake connection requirement or the "No saved profiles" notice as in code.
5. Caption `Suggested: X of Y columns selected` sourced from `selection_counts` showing immediately below the control row.
6. Action row with three columns sized `[1, 1, 2]`:
   • Column 1 hosts the primary button "▶️ Run Profile" (disabled when a saved run is loaded).
   • Column 2 hosts the secondary button "✨ Suggest DQ Config" (disabled until profile results exist).
   • Column 3 shows an info box `Viewing a saved profile.` plus a "Clear loaded profile" button only when `profile_loaded_run_id` is set; otherwise it must remain empty.
7. Metrics row of three `st.metric` widgets labelled "Rows profiled", "Sampling", and "Duration".  These appear once results exist and must remain in this order.
8. Optional warning banner firing for full scans over the threshold with the message "Full table scan processed {rows} rows. Consider sampling to improve performance."  The icon stays `⚠️`.
9. Filters container that starts with subheader "Filters" then four toggles: "High null % (>20%)", "Unique candidates", "Low cardinality", "Whitespace risk".  Immediately afterwards render the bold label "Semantic tags" and a single row of checkboxes: "Identifiers", "Financial", "Instrument", "Geo", "Contact", "Date (Text)", "Reference Codes"—all defaulting to `False`.
10. Toggle "💾 Save Profile" (key `profile_save_toggle`) with dynamic help text explaining persistence.  This control is always rendered; it is disabled rather than hidden when metadata prerequisites fail.
11. Selection editor grid created with `st.data_editor` that exposes only two columns: `Select` (checkbox) and `Column` (read-only).  The editor must precede the main dataframe and use key `profile_results_selection`.
12. Main profile dataframe displayed with `st.dataframe` using the styled `grid_df`.  Required columns in order: `Select`, `Column`, `Physical Type`, `Nulls`, `Distinct`, `Avg Length`, `Min Value`, `Max Value`, `Whitespace %`, `Guessed Type`, `Confidence`, `Note`.  Confidence styling and legend text (green ≥90%, amber 75–89, grey otherwise) must remain untouched.
13. Subheader "Top values by column" followed by one expander per column in the filtered results.  Each expander title follows the pattern `<column> (<N values>)`.  Inside, render exactly one `st.table` using the processed `tv_df` (columns "Value", "Count", and any metadata-supplied percentage columns).  When the table is empty, show the existing informational messages instead of alternative layouts.

Forbidden patterns:
• Do not add, remove, or reorder the control groups above.
• Do not introduce extra tabs, accordions, or secondary grids beyond the selection editor, main dataframe, and per-column tables described.
• Do not alter widget labels, keys, or default states except through existing logic.
• Do not surface additional persistence toggles or sampling controls; `💾 Save Profile` and `Sample %` are the sole persistence and sampling mechanisms.
