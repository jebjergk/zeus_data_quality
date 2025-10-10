"""Streamlit entry point providing lightweight routing between Zeus DQ pages."""

import re

import streamlit as st

try:
    from snowflake.snowpark.context import get_active_session

    session = get_active_session()
except Exception:  # pragma: no cover - Snowpark not available locally
    session = None

from pages import configs, docs, home, monitor

st.set_page_config(page_title="Zeus Data Quality", layout="wide")

GLOBAL_CSS = """
<style>
.sf-hr { height:1px; background:#e7ebf3; border:0; margin:.6rem 0 1rem 0; }
.badge { display:inline-block; padding:.15rem .55rem; border-radius:999px; font-size:.75rem; font-weight:600;
 background:#e5f6fd; color:#055e86; border:1px solid #cbeefb; }
.badge-green { background:#eafaf0; border-color:#d4f2df; color:#0a5c2b; }
.card { border:1px solid #e7ebf3; border-radius:12px; padding:.9rem 1rem; background:#fff; box-shadow:0 1px 2px rgba(12,18,28,.04); }
.small { font-size:.85rem; color:#6b7280; }
.kv { color:#111827; font-weight:600; }
section[data-testid="stSidebar"] .stButton>button {
 width:100%;
 border-radius:10px;
}
section[data-testid="stSidebar"] .stButton {
 margin-bottom:.4rem;
}
</style>
"""

MONITOR_FILTER_CSS = """
<style>
form[data-testid="stForm"][aria-label="mon_filters"] div[data-testid="stWidget"] {
    margin-bottom: 0.4rem;
}
.pill { display:inline-block; padding:2px 8px; border-radius:10px; font-size:12px; background:#eef3ff; }
</style>
"""

st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

if "page" not in st.session_state:
    st.session_state["page"] = "home"

NAV_ITEMS = (
    ("Overview", "home", home.render_home),
    ("Configurations", "configs", configs.render_configs),
    ("Monitor", "monitor", monitor.render_monitor),
    ("Documentation", "docs", docs.render_docs),
)


def _sanitize_id(value: str) -> str:
    """Return an uppercase identifier containing only alphanumerics and underscores."""

    return re.sub(r"[^A-Z0-9_]", "_", (value or "").upper())



def navigate_to(page: str) -> None:
    """Set the active page and rerun the app."""

    st.session_state["page"] = page
    st.experimental_rerun()


with st.container():
    nav_cols = st.columns(len(NAV_ITEMS))
    for col, (label, page_key, _) in zip(nav_cols, NAV_ITEMS):
        button_type = "primary" if st.session_state["page"] == page_key else "secondary"
        if col.button(label, key=f"nav_{page_key}", type=button_type):
            st.session_state["page"] = page_key
            st.experimental_rerun()

current_page = st.session_state.get("page", "home")
for label, page_key, renderer in NAV_ITEMS:
    if current_page == page_key:
        renderer(session)
        break
