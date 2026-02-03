import os

import streamlit as st


def check_auth() -> bool:
    """Returns True if auth is disabled or user is authenticated."""
    password = os.getenv("DASHBOARD_PASSWORD", "")
    if not password:
        return True  # No password set = no auth required

    if st.session_state.get("authenticated"):
        return True

    st.title("FluxTrade Login")
    entered = st.text_input("Password", type="password")
    if st.button("Login"):
        if entered == password:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Wrong password")
    return False
