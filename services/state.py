import streamlit as st

def get_state():
    if "run_as_role" not in st.session_state: st.session_state["run_as_role"] = None
    if "dmf_role" not in st.session_state: st.session_state["dmf_role"] = None
    return {"run_as_role": st.session_state["run_as_role"], "dmf_role": st.session_state["dmf_role"]}

def set_state(run_as_role, dmf_role):
    st.session_state["run_as_role"] = run_as_role
    st.session_state["dmf_role"] = dmf_role
