import os

import streamlit as st


def check_auth() -> bool:
    """Return True if auth is disabled or the user is authenticated."""
    password = os.getenv("DASHBOARD_PASSWORD", "")
    auth_required = os.getenv("DASHBOARD_AUTH_REQUIRED", "").lower() in {
        "1",
        "true",
        "yes",
    }
    if not password:
        if auth_required:
            st.error("DASHBOARD_PASSWORD is required")
            return False
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
