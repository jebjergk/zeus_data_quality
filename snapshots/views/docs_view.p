"""UI CONTRACT – DO NOT CHANGE WITHOUT EXPLICIT INSTRUCTION

Layout requirements:
1. Page header "Zeus Data Quality documentation" appears once at the top.
2. A fixed `st.tabs` call defines six tabs in this exact order: "User Guide", "Profiling", "DQ Framework", "Technical Overview",
 "Data Governance", "Version History".  Tab labels and count must not change.
3. Tab content expectations:
   • "User Guide" tab renders the subheader "User Guide" followed by the existing markdown sections (What the app delivers, Creat
 e a configuration, Checks in plain language, Run and monitor results, Troubleshooting essentials).  No interactive widgets belo
ng in this tab.
   • "Profiling" tab renders the subheader "Profiling" and the markdown sections covering why to profile, metrics, semantic insig
hts, filters, performance notes, and persistence guidance exactly as shipped.  This tab remains markdown-only.
   • "DQ Framework" tab renders the subheader "Snowflake-native data quality framework" with descriptive markdown, then a divider
, then subheader "Entity Diagram" with a Graphviz diagram (rendered via `st.graphviz_chart`) describing metadata objects, follow
e d by subheader "Workflow Diagram" with its Graphviz diagram.  Chart ordering and titles must remain unchanged.
   • "Technical Overview" tab renders the subheader "Technical Overview", markdown with runtime/metadata/procedure details, then
 a `st.markdown` heading "#### Required privileges for the caller role" and a single `st.code` block listing privilege statements
.  No additional controls may be added.
   • "Data Governance" tab renders the subheader "Data Governance and Security" plus the markdown sections on roles, traceability
, access, and sensitive data handling.
   • "Version History" tab renders the subheader "Version History" followed by the info message "Version history will be documente
d here once releases are tracked."  No other content is permitted.

Forbidden patterns:
• Do not change the tab order, labels, or count.
• Do not replace markdown content with inputs, tables, or dataframes.
• Do not remove or reposition the Graphviz charts within the "DQ Framework" tab.
• Do not add new alerts, buttons, or accordions to any tab without explicit approval.
"""

"""Documentation view rendering helpers."""

from __future__ import annotations

from typing import Tuple

import streamlit as st

from ui.strings import DocsStrings
from utils.flags import DEMO_LOCK, UI_CONTRACT_STRICT
from utils.meta import _q


def _contract_message(message: str) -> None:
    """Display contract feedback as warning or error based on strict mode."""

    strict = UI_CONTRACT_STRICT or DEMO_LOCK
    if strict:
        st.error(message)
    else:
        st.warning(message)


def _safe_quote(identifier: str) -> str:
    """Return a safely quoted identifier for display purposes."""

    try:
        return _q(identifier)
    except Exception:
        return identifier


def _sanitize_graph_label(label: str) -> Tuple[str, str]:
    """Sanitize a fully qualified name for use in graph visuals."""

    node_name = "".join(ch if ch.isalnum() else "_" for ch in label)
    if not node_name:
        node_name = "node"
    display = _safe_quote(label).replace('"', "\\\"")
    return node_name, display


def render_docs(
    metadata_db: str,
    metadata_schema: str,
    proc_name: str,
    configs_table: str,
    checks_table: str,
    run_results_table: str,
) -> None:
    """Render the documentation tabs for the Streamlit application."""

    st.header(DocsStrings.HEADER)

    tabs = st.tabs(list(DocsStrings.TABS))

    if len(tabs) != 6:
        _contract_message(DocsStrings.CONTRACT_TABS_MISMATCH)
        return

    cfg_tbl_display = _safe_quote(configs_table)
    chk_tbl_display = _safe_quote(checks_table)
    run_results_display = _safe_quote(run_results_table)
    metadata_db_display = _safe_quote(metadata_db)
    metadata_schema_display = _safe_quote(metadata_schema)

    cfg_tbl_node, cfg_tbl_label = _sanitize_graph_label(configs_table)
    chk_tbl_node, chk_tbl_label = _sanitize_graph_label(checks_table)
    run_tbl_node, run_tbl_label = _sanitize_graph_label(run_results_table)
    proc_node, proc_label = _sanitize_graph_label(proc_name)

    profile_run_node, profile_run_label = _sanitize_graph_label(
        f"{metadata_db}.{metadata_schema}.DQ_PROFILE_RUN"
    )
    profile_col_node, profile_col_label = _sanitize_graph_label(
        f"{metadata_db}.{metadata_schema}.DQ_PROFILE_COLUMN"
    )

    with tabs[0]:
        st.subheader(DocsStrings.USER_GUIDE_SUBHEADER)
        st.markdown(DocsStrings.USER_GUIDE_CONTENT)

    with tabs[1]:
        st.subheader(DocsStrings.PROFILING_SUBHEADER)
        st.markdown(DocsStrings.PROFILING_CONTENT)

    with tabs[2]:
        st.subheader(DocsStrings.FRAMEWORK_SUBHEADER)
        st.markdown(
            DocsStrings.FRAMEWORK_CONTENT.format(
                metadata_db=metadata_db_display,
                metadata_schema=metadata_schema_display,
                run_results_table=run_results_display,
                proc_name=proc_name,
            )
        )

        st.divider()
        st.subheader("Entity Diagram")
        entity_graph = DocsStrings.FRAMEWORK_ENTITY_GRAPH.format(
            cfg_tbl_node=cfg_tbl_node,
            cfg_tbl_label=cfg_tbl_label,
            chk_tbl_node=chk_tbl_node,
            chk_tbl_label=chk_tbl_label,
            run_tbl_node=run_tbl_node,
            run_tbl_label=run_tbl_label,
            profile_run_node=profile_run_node,
            profile_run_label=profile_run_label,
            profile_col_node=profile_col_node,
            profile_col_label=profile_col_label,
            proc_node=proc_node,
            proc_label=proc_label,
        )
        st.graphviz_chart(entity_graph, use_container_width=True)

        st.subheader("Workflow Diagram")
        st.graphviz_chart(DocsStrings.FRAMEWORK_WORKFLOW_GRAPH, use_container_width=True)

    with tabs[3]:
        st.subheader(DocsStrings.TECHNICAL_SUBHEADER)
        st.markdown(
            DocsStrings.TECHNICAL_CONTENT.format(
                cfg_tbl_display=cfg_tbl_display,
                chk_tbl_display=chk_tbl_display,
                run_results_table=run_results_display,
                metadata_db=metadata_db_display,
                metadata_schema=metadata_schema_display,
                proc_name=proc_name,
            )
        )

        st.markdown(DocsStrings.TECHNICAL_PRIVILEGES_HEADING)
        st.code(
            DocsStrings.TECHNICAL_PRIVILEGES_SNIPPET.format(
                metadata_db=metadata_db_display,
                metadata_schema=metadata_schema_display,
                proc_name=proc_name,
            ),
            language="text",
        )

    with tabs[4]:
        st.subheader(DocsStrings.DATA_GOVERNANCE_SUBHEADER)
        st.markdown(DocsStrings.DATA_GOVERNANCE_CONTENT)

    with tabs[5]:
        st.subheader(DocsStrings.VERSION_HISTORY_SUBHEADER)
        st.info(DocsStrings.VERSION_HISTORY_INFO)
