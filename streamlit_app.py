import re
from pathlib import Path

import streamlit as st

try:
    from snowflake.snowpark.context import get_active_session

    session = get_active_session()
except Exception:
    session = None

from utils.meta import DQConfig
from pages import configs, docs, home, monitor

st.set_page_config(page_title="Zeus Data Quality", layout="wide")

GLOBAL_CSS = """
<style>
.sf-hr { height:1px; background:#e7ebf3; border:0; margin:.6rem 0 1rem 0; }
.badge { display:inline-block; padding:.15rem .55rem; border-radius:999px; font-size:.75rem; font-weight:600; background:#e5f6fd; color:#055e86; border:1px solid #cbeefb; }
.badge-green { background:#eafaf0; border-color:#d4f2df; color:#0a5c2b; }
.card { border:1px solid #e7ebf3; border-radius:12px; padding:.9rem 1rem; background:#fff; box-shadow:0 1px 2px rgba(12,18,28,.04); }
.small { font-size:.85rem; color:#6b7280; }
.kv { color:#111827; font-weight:600; }
.pill { display:inline-flex; align-items:center; padding:.2rem .65rem; border-radius:999px; font-size:.75rem; font-weight:600; background:#eef3ff; border:1px solid #d7e1ff; color:#1f3a8a; }
.top-nav { display:flex; flex-wrap:wrap; gap:.5rem; margin-bottom:1.25rem; }
.top-nav .stButton>button { border-radius:999px; padding:0.4rem 1.1rem; }
section[data-testid="stSidebar"] .stButton>button { width:100%; border-radius:10px; }
section[data-testid="stSidebar"] .stButton { margin-bottom:.4rem; }
form[data-testid="stForm"][aria-label="mon_filters"] div[data-testid="stWidget"] { margin-bottom:0.4rem; }
main .block-container { padding-top:1.25rem; }
</style>
"""


def _read_version(path: str = "version.txt") -> str:
    """Return the application version from file or a DEV fallback."""

    version_path = Path(path)
    try:
        if version_path.is_file():
            for line in version_path.read_text(encoding="utf-8").splitlines():
                candidate = line.strip()
                if candidate:
                    return candidate
    except OSError:
        pass
    return "DEV"


APP_VERSION = _read_version()


def get_app_version() -> str:
    """Expose the resolved application version to downstream pages."""

    return APP_VERSION


st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


def _keyify(s: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in s).lower()


def _sanitize_id(s: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "_", (s or "").upper())


def _quote_ident(value: str) -> str:
    safe = value.replace('"', '""')
    return f'"{safe}"'


def _task_fqn_for_config(cfg: DQConfig) -> str:
    target_fqn = (cfg.target_table_fqn or "").strip()
    if not target_fqn:
        raise ValueError("Target table FQN missing database or schema")

    parts = [p.strip().strip('"') for p in target_fqn.split(".") if p.strip()]
    if len(parts) < 2:
        raise ValueError("Target table FQN missing database or schema")

    db_name, schema_name = parts[0], parts[1]
    cfg_id = str(cfg.config_id).replace('"', '')
    task_identifier = f"DQ_TASK_{cfg_id}"
    return ".".join([
        _quote_ident(db_name),
        _quote_ident(schema_name),
        _quote_ident(task_identifier),
    ])


if "page" not in st.session_state:
    st.session_state["page"] = "home"

nav_items = [
    ("Overview", "home"),
    ("Configurations", "configs"),
    ("Monitor", "monitor"),
    ("Documentation", "docs"),
]

nav_cols = st.columns(len(nav_items))
for col, (label, page_key) in zip(nav_cols, nav_items):
    is_active = st.session_state["page"] == page_key
    button_type = "primary" if is_active else "secondary"
    if col.button(label, key=f"nav_{page_key}", type=button_type):
        if page_key == "configs":
            configs.reset_config_editor_state()
        st.session_state["page"] = page_key

current_page = st.session_state.get("page", "home")

if current_page == "configs":
    configs.render_configs(session, app_version=APP_VERSION)
elif current_page == "monitor":
    monitor.render_monitor(session, app_version=APP_VERSION)
elif current_page == "docs":
    docs.render_docs(session, app_version=APP_VERSION)
else:
    st.session_state["page"] = "home"
    home.render_home(session, app_version=APP_VERSION)
