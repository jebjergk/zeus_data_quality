"""Documentation page for Zeus Data Quality."""

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

import streamlit as st

from pages import ui_shared
from utils.checkdefs import SUPPORTED_COLUMN_CHECKS, SUPPORTED_TABLE_CHECKS
from utils.meta import (
    DQ_CHECK_TBL,
    DQ_CONFIG_TBL,
    DQ_RUN_RESULTS_TBL,
    fq_table,
    metadata_db_schema,
)


_COLUMN_CHECK_DESCRIPTIONS = {
    "UNIQUE": "Each non-null value appears only once in the target column.",
    "NULL_COUNT": "The column is free from NULLs unless explicitly allowed.",
    "MIN_MAX": "Values stay within configured minimum and maximum thresholds.",
    "WHITESPACE": "String content respects trimming or whitespace expectations.",
    "FORMAT_DISTRIBUTION": "Values conform to a supplied regular expression pattern.",
    "VALUE_DISTRIBUTION": "Values belong to a curated list provided at configuration time.",
}

_TABLE_CHECK_DESCRIPTIONS = {
    "FRESHNESS": "Latest timestamp in the table remains within an acceptable age window.",
    "ROW_COUNT": "The table contains at least the configured minimum number of rows.",
    "ROW_COUNT_ANOMALY": "Current row count is in line with historical behaviour using MAD tolerance.",
}

_METADATA_OBJECTS: Tuple[Tuple[str, str, str], ...] = (
    (
        "Configurations",
        DQ_CONFIG_TBL,
        "Stores high-level rule bundles mapped to physical data sets.",
    ),
    (
        "Checks",
        DQ_CHECK_TBL,
        "Holds individual validation rules derived from each configuration.",
    ),
    (
        "Run results",
        DQ_RUN_RESULTS_TBL,
        "Captures execution outcomes, metrics, and failure samples per check run.",
    ),
)


def _resolve_metadata_fqns(session) -> Tuple[str, str, Dict[str, Tuple[str, str, str]]]:
    """Resolve fully-qualified metadata table names."""

    db = schema = None
    try:
        if session:
            db, schema = metadata_db_schema(session)
    except Exception:
        db = schema = None

    resolved: Dict[str, Tuple[str, str, str]] = {}
    for label, table_name, description in _METADATA_OBJECTS:
        resolved[label] = (table_name, _qualify_table(table_name, db, schema), description)
    return db or "", schema or "", resolved


def _qualify_table(table_name: str, db: str | None, schema: str | None) -> str:
    trimmed = table_name.strip().strip('"')
    if not trimmed:
        return table_name

    parts = [p.strip() for p in trimmed.split(".") if p.strip()]
    if len(parts) >= 3:
        quoted_parts = []
        for part in parts:
            cleaned = part.strip('"')
            quoted_parts.append(f'"{cleaned}"')
        return ".".join(quoted_parts)
    if len(parts) == 2 and db:
        return fq_table(db, parts[0], parts[1])
    if db and schema:
        return fq_table(db, schema, trimmed)
    if schema:
        return f"{schema}.{trimmed}"
    return trimmed


def _fetch_table_counts(session, fqns: Iterable[Tuple[str, str]]) -> Dict[str, str]:
    counts: Dict[str, str] = {}
    if not session:
        return counts

    for label, fqn in fqns:
        try:
            result = session.sql(f"SELECT COUNT(*) AS CNT FROM {fqn}").collect()[0]
            if hasattr(result, "asDict"):
                counts[label] = str(result.asDict().get("CNT") or result.asDict().get("COUNT(*)") or 0)
            else:
                counts[label] = str(result[0])
        except Exception as exc:  # pragma: no cover - depends on live session
            counts[label] = f"Error: {exc}"
    return counts


def _build_verification_sql(fqns: Dict[str, Tuple[str, str, str]]) -> str:
    config_fqn = fqns["Configurations"][1]
    check_fqn = fqns["Checks"][1]
    results_fqn = fqns["Run results"][1]
    return "\n".join(
        [
            f"SELECT COUNT(*) AS config_rows FROM {config_fqn};",
            f"SELECT config_id, COUNT(*) AS checks_per_config FROM {check_fqn} GROUP BY 1 ORDER BY 2 DESC;",
            f"SELECT status, COUNT(*) AS runs FROM {results_fqn} GROUP BY 1 ORDER BY 2 DESC;",
        ]
    )


def _build_markdown(
    db: str,
    schema: str,
    fqns: Dict[str, Tuple[str, str, str]],
    counts: Dict[str, str],
    verification_sql: str,
) -> str:
    usage_block = "\n".join(
        [
            "## Usage",
            "1. **Register** target tables on the Configurations page and describe the business intent.",
            "2. **Attach checks** by selecting column- and table-level rules that reflect your policy.",
            "3. **Schedule or trigger** executions directly from Snowflake tasks or manual runs.",
            "4. **Monitor outcomes** on the Monitor page to investigate failures and share insights.",
        ]
    )

    objects_header = ["## Metadata objects", "| Object | Fully-qualified name | Purpose |", "| --- | --- | --- |"]
    for label, (_, fqn, description) in fqns.items():
        objects_header.append(f"| {label} | `{fqn}` | {description} |")
    objects_block = "\n".join(objects_header)

    column_lines = ["### Column checks"]
    for check in SUPPORTED_COLUMN_CHECKS:
        desc = _COLUMN_CHECK_DESCRIPTIONS.get(check, "")
        column_lines.append(f"- **{check}** — {desc}")
    table_lines = ["### Table checks"]
    for check in SUPPORTED_TABLE_CHECKS:
        desc = _TABLE_CHECK_DESCRIPTIONS.get(check, "")
        table_lines.append(f"- **{check}** — {desc}")
    checks_block = "\n".join(["## Checks library", *column_lines, "", *table_lines])

    permissions_block = "\n".join(
        [
            "## Permissions",
            "- `USAGE` on the Snowflake warehouse powering executions.",
            "- `USAGE` on the database and schema that host the metadata tables.",
            "- `SELECT` on the target tables being validated.",
            "- `EXECUTE TASK` or ability to create tasks if automated scheduling is enabled.",
            "- Optional: access to Data Metric Functions when DMF roles are configured.",
        ]
    )

    svg_block = "\n".join(
        [
            "## Architecture overview",
            "<div style=\"text-align:center;\">",
            "<svg viewBox=\"0 0 600 220\" xmlns=\"http://www.w3.org/2000/svg\" width=\"100%\">",
            "  <defs>",
            "    <linearGradient id=\"appGrad\" x1=\"0%\" y1=\"0%\" x2=\"100%\" y2=\"0%\">",
            "      <stop offset=\"0%\" stop-color=\"#4f46e5\" />",
            "      <stop offset=\"100%\" stop-color=\"#38bdf8\" />",
            "    </linearGradient>",
            "    <marker id=\"arrowhead\" markerWidth=\"8\" markerHeight=\"8\" refX=\"4\" refY=\"4\" orient=\"auto\">",
            "      <polygon points=\"0 0, 8 4, 0 8\" fill=\"#64748b\" />",
            "    </marker>",
            "  </defs>",
            "  <rect x=\"30\" y=\"40\" width=\"160\" height=\"60\" rx=\"12\" fill=\"url(#appGrad)\" opacity=\"0.85\" />",
            "  <text x=\"110\" y=\"75\" font-size=\"14\" text-anchor=\"middle\" fill=\"#ffffff\">Streamlit UI</text>",
            "  <rect x=\"220\" y=\"20\" width=\"160\" height=\"60\" rx=\"12\" fill=\"#0f172a\" opacity=\"0.85\" />",
            "  <text x=\"300\" y=\"55\" font-size=\"14\" text-anchor=\"middle\" fill=\"#f8fafc\">Snowpark Session</text>",
            "  <rect x=\"220\" y=\"110\" width=\"160\" height=\"80\" rx=\"12\" fill=\"#f1f5f9\" stroke=\"#94a3b8\" />",
            "  <text x=\"300\" y=\"145\" font-size=\"13\" text-anchor=\"middle\" fill=\"#0f172a\">Metadata Schema</text>",
            "  <text x=\"300\" y=\"165\" font-size=\"12\" text-anchor=\"middle\" fill=\"#0f172a\">Configs · Checks · Results</text>",
            "  <rect x=\"420\" y=\"40\" width=\"160\" height=\"60\" rx=\"12\" fill=\"#0ea5e9\" opacity=\"0.8\" />",
            "  <text x=\"500\" y=\"75\" font-size=\"14\" text-anchor=\"middle\" fill=\"#0f172a\">Target Tables</text>",
            "  <line x1=\"190\" y1=\"70\" x2=\"220\" y2=\"50\" stroke=\"#64748b\" stroke-width=\"2\" marker-end=\"url(#arrowhead)\" />",
            "  <line x1=\"380\" y1=\"50\" x2=\"420\" y2=\"70\" stroke=\"#64748b\" stroke-width=\"2\" marker-end=\"url(#arrowhead)\" />",
            "  <line x1=\"300\" y1=\"80\" x2=\"300\" y2=\"110\" stroke=\"#64748b\" stroke-width=\"2\" marker-end=\"url(#arrowhead)\" />",
            "</svg>",
            "</div>",
        ]
    )

    counts_lines: List[str] = ["## Live metadata counts"]
    if counts:
        for label, value in counts.items():
            counts_lines.append(f"- **{label}**: {value}")
    else:
        counts_lines.append(
            "- Connect a Snowflake session to surface live row counts for the metadata tables."
        )
    counts_block = "\n".join(counts_lines)

    verification_block = "\n".join(
        [
            "## Verification SQL",
            "Run the queries below inside Snowflake to sanity-check the metadata footprint.",
            "```sql",
            verification_sql,
            "```",
        ]
    )

    context_block = "\n".join(
        [
            "## Context",
            f"Active metadata schema: `{db + '.' if db else ''}{schema}`" if schema else "Metadata schema will be resolved once connected.",
        ]
    )

    return "\n\n".join(
        [
            "# Zeus Data Quality documentation",
            context_block,
            usage_block,
            objects_block,
            checks_block,
            permissions_block,
            svg_block,
            counts_block,
            verification_block,
        ]
    )


def render_docs(session) -> None:
    """Render the documentation page."""

    ui_shared.page_header("Documentation", session=session, show_version=True)
    db, schema, fqns = _resolve_metadata_fqns(session)
    counts = _fetch_table_counts(session, [(label, data[1]) for label, data in fqns.items()])
    verification_sql = _build_verification_sql(fqns)
    markdown = _build_markdown(db, schema, fqns, counts, verification_sql)

    st.markdown(markdown, unsafe_allow_html=True)
    st.download_button(
        "Download as Markdown",
        data=markdown.encode("utf-8"),
        file_name="zeus_dq_documentation.md",
        mime="text/markdown",
    )
