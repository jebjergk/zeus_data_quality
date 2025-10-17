from __future__ import annotations

from typing import List, Optional, Tuple

import streamlit as st

from utils.meta import fq_table, list_databases, list_schemas, list_tables


def session_cache_token(session_obj) -> str:
    """Return a stable identifier for the given Snowpark session."""

    for attr in ("session_id", "_session_id"):
        token = getattr(session_obj, attr, None)
        if token:
            return str(token)
    getter = getattr(session_obj, "get_session_id", None)
    if callable(getter):
        try:
            token = getter()
            if token:
                return str(token)
        except Exception:
            pass
    return str(id(session_obj))


def _list_databases_cached(session_obj) -> List[str]:
    if not session_obj:
        return []

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_databases(cache_token: str) -> List[str]:
        try:
            return list_databases(session_obj)
        except Exception as exc:  # pragma: no cover - Snowflake specific
            st.error(f"Failed to list databases: {exc}")
            return []

    return _load_databases(session_cache_token(session_obj))


def _list_schemas_cached(session_obj, database: str) -> List[str]:
    if not session_obj or not database:
        return []

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_schemas(cache_token: Tuple[str, str]) -> List[str]:
        _, db_name = cache_token
        try:
            return list_schemas(session_obj, db_name)
        except Exception as exc:  # pragma: no cover - Snowflake specific
            st.error(f"Failed to list schemas for {db_name}: {exc}")
            return []

    return _load_schemas((session_cache_token(session_obj), database))


def _list_tables_cached(session_obj, database: str, schema: str) -> List[str]:
    if not session_obj or not (database and schema):
        return []

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_tables(cache_token: Tuple[str, str, str]) -> List[str]:
        _, db_name, schema_name = cache_token
        try:
            return list_tables(session_obj, db_name, schema_name)
        except Exception as exc:  # pragma: no cover - Snowflake specific
            st.error(f"Failed to list tables for {db_name}.{schema_name}: {exc}")
            return []

    return _load_tables((session_cache_token(session_obj), database, schema))


def stateless_table_picker(session_obj, preselect_fqn: Optional[str]):
    """Simple, stateless DB → Schema → Table picker. Returns (db, schema, table, fqn)."""

    def split_fqn(fqn):
        if not fqn or fqn.count(".") != 2:
            return None, None, None
        a, b, c = [p.strip('"') for p in fqn.split(".")]
        return a, b, c

    pre_db, pre_sch, pre_tbl = split_fqn(preselect_fqn or "")

    dbs = _list_databases_cached(session_obj)
    db_index = (
        next((i for i, v in enumerate(dbs) if pre_db and v.upper() == pre_db.upper()), 0)
        if dbs
        else 0
    )
    db_sel = st.selectbox("Database", dbs or ["— none —"], index=db_index if dbs else 0)
    if not dbs or db_sel == "— none —":
        return None, None, None, ""

    schemas = _list_schemas_cached(session_obj, db_sel)
    sch_index = (
        next((i for i, v in enumerate(schemas) if pre_sch and v.upper() == pre_sch.upper()), 0)
        if schemas
        else 0
    )
    sch_sel = st.selectbox("Schema", schemas or ["— none —"], index=sch_index if schemas else 0)
    if not schemas or sch_sel == "— none —":
        return db_sel, None, None, ""

    tables = _list_tables_cached(session_obj, db_sel, sch_sel)
    tbl_index = (
        next((i for i, v in enumerate(tables) if pre_tbl and v.upper() == pre_tbl.upper()), 0)
        if tables
        else 0
    )
    tbl_sel = st.selectbox("Table", tables or ["— none —"], index=tbl_index if tables else 0)
    if not tables or tbl_sel == "— none —":
        return db_sel, sch_sel, None, ""

    return db_sel, sch_sel, tbl_sel, fq_table(db_sel, sch_sel, tbl_sel)
