import streamlit as st
from ui import (
    user_submit_ticket,
    user_my_tickets,
    user_chat_ticket,
    admin_dashboard,
    admin_tickets,
    admin_search_kb,
)

st.set_page_config(page_title="Janus", layout="wide")

st.sidebar.title("Janus")
page = st.sidebar.radio(
    "Navigate",
    [
        "✉️ Submit Ticket",
        "📁 My Tickets",
        "💬 Chat with AI",
        "📊 Admin Dashboard",
        "🧾 Manage Tickets",
        "🔍 Search KB",
    ],
)

if page == "✉️ Submit Ticket":
    user_submit_ticket.run()
elif page == "📁 My Tickets":
    user_my_tickets.run()
elif page == "💬 Chat with AI":
    user_chat_ticket.run()
elif page == "📊 Admin Dashboard":
    admin_dashboard.run()
elif page == "🧾 Manage Tickets":
    admin_tickets.run()
elif page == "🔍 Search KB":
    admin_search_kb.run()
