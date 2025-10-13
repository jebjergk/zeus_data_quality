# services/state.py
import streamlit as st
from typing import Any, Dict, Optional

SESSION_KEYS = ["run_as_role", "dmf_role"]

def get_state() -> Dict[str, Any]:
    for k in SESSION_KEYS:
        if k not in st.session_state:
            st.session_state[k] = None
    return {k: st.session_state[k] for k in SESSION_KEYS}

def set_state(run_as_role: Optional[str], dmf_role: Optional[str]) -> None:
    st.session_state["run_as_role"] = run_as_role
    st.session_state["dmf_role"] = dmf_role
