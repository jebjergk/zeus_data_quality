UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Layout requirements:
1. Page header "Zeus Data Quality documentation" appears once at the top.
2. A fixed `st.tabs` call defines six tabs in this exact order: "User Guide", "Profiling", "DQ Framework", "Technical Overview", "Data Governance", "Version History".  Tab labels and count must not change.
3. Tab content expectations:
   • "User Guide" tab renders the subheader "User Guide" followed by the existing markdown sections (What the app delivers, Create a configuration, Checks in plain language, Run and monitor results, Troubleshooting essentials).  No interactive widgets belong in this tab.
   • "Profiling" tab renders the subheader "Profiling" and the markdown sections covering why to profile, metrics, semantic insights, filters, performance notes, and persistence guidance exactly as shipped.  This tab remains markdown-only.
   • "DQ Framework" tab renders the subheader "Snowflake-native data quality framework" with descriptive markdown, then a divider, then subheader "Entity Diagram" with a Graphviz diagram (rendered via `st.graphviz_chart`) describing metadata objects, followed by subheader "Workflow Diagram" with its Graphviz diagram.  Chart ordering and titles must remain unchanged.
   • "Technical Overview" tab renders the subheader "Technical Overview", markdown with runtime/metadata/procedure details, then a `st.markdown` heading "#### Required privileges for the caller role" and a single `st.code` block listing privilege statements.  No additional controls may be added.
   • "Data Governance" tab renders the subheader "Data Governance and Security" plus the markdown sections on roles, traceability, access, and sensitive data handling.
   • "Version History" tab renders the subheader "Version History" followed by the info message "Version history will be documented here once releases are tracked."  No other content is permitted.

Forbidden patterns:
• Do not change the tab order, labels, or count.
• Do not replace markdown content with inputs, tables, or dataframes.
• Do not remove or reposition the Graphviz charts within the "DQ Framework" tab.
• Do not add new alerts, buttons, or accordions to any tab without explicit approval.
