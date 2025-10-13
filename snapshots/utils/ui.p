# utils/ui.py
import streamlit as st
from typing import List, Optional, Dict, Any

SAFE_EMPTY = st.secrets.get("SAFE_EMPTY_LABEL", "— none —") if hasattr(st, 'secrets') else "— none —"

def safe_select(label: str, options: List[str], index: Optional[int] = None, key: Optional[str] = None) -> Optional[str]:
    opts = [SAFE_EMPTY] + options
    idx = 0 if index is None else index + 1
    choice = st.selectbox(label, opts, index=idx, key=key)
    return None if choice == SAFE_EMPTY else choice

def safe_multiselect(label: str, options: List[str], default: Optional[List[str]] = None, key: Optional[str] = None) -> List[str]:
    default = default or []
    default_display = [o for o in default if o in options]
    vals = st.multiselect(label, options, default=default_display, key=key)
    return list(vals)

def highlight_active(row_status: str) -> Dict[str, Any]:
    if (row_status or '').upper() == 'ACTIVE':
        return {"backgroundColor": "#eaffea"}
    return {}
