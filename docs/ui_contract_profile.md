UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Controls (top to bottom):
1. Page header labelled "🧪 Profiling" with the stateless table picker rendered directly beneath it. The picker uses three selectors (database, schema, table) sourced from `stateless_table_picker` and must persist the selected fully qualified name in `st.session_state["profile_target_fqn"]`.
2. A horizontal divider separating the picker from the controls.
3. Control row rendered as two equal columns:
   • Column 1 hosts the primary button "▶️ Run profiling" (disabled until a table is selected).
   • Column 2 hosts the secondary button "🔄 Refresh results" (disabled until a table is selected).
   The status placeholder that reports success or failure must appear immediately below this row.
4. When no table is selected, display the informational message "Use the database, schema, and table selectors above to choose a target table." and stop rendering additional sections.
5. Once a table is selected, show `st.success("Profiling target: <FQN>")` followed by a spinner that loads metadata using `services.profiling_v2` helpers.  After the spinner completes:
   • Render the summary subheader "Latest profile summary".
   • Display three `st.metric` widgets labelled "Rows profiled", "Sample", and "Duration" in that order.  The duration metric tooltip must include the formatted timestamp as implemented.
6. Results section uses `st.tabs` with four tabs in this exact order: "Column features", "Semantic tags", "Suggested checks", "Run history".  Tab labels cannot change.
   • Tab 1 (`Column features`) shows the subheader "Column statistics" and a dataframe of `DQ_COLUMN_FEATURES` results or the info message "No column metrics found. Run profiling to populate statistics.".
   • Tab 2 (`Semantic tags`) shows the subheader "Semantic classification" and a dataframe of `DQ_COLUMN_CLASSIFICATION` results or the info message "No semantic classifications recorded for this table.".
   • Tab 3 (`Suggested checks`) shows the subheader "Recommended data quality checks" and a dataframe of `DQ_SUGGESTED_CHECKS` results or the info message "No suggested checks available for the current profile.".
   • Tab 4 (`Run history`) shows the subheader "Recent profiling runs" and a dataframe of `DQ_PROFILE_RUN` results or the info message "No profiling runs have been logged yet.".
7. When `DEBUG_PROFILING` is true, wrap the metadata payload in a single expander titled "Debug · Profiling payload".  The expander must be collapsed by default.

Forbidden patterns:
• Do not add sampling sliders, saved profile controls, or suggestion buttons outside the metadata tabs described above.
• Do not introduce additional tabs, tables, or metrics beyond the summary row and the four required tabs.
• Do not read source tables directly from the UI; all data must come from ZEUS_ANALYTICS_SIMU.DISCOVERY metadata objects via `services.profiling_v2`.
