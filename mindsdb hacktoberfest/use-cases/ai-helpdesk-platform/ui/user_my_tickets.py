import streamlit as st
from core.json_store import JsonStore

store = JsonStore()


def run():
    st.subheader("My Tickets")

    all_tickets = store.list_tickets()
    tickets = [ticket for ticket in all_tickets if ticket.get('status') == 'open']

    if not tickets:
        st.info("No tickets submitted yet.")
        return

    priority_colors = {
        "High": "red",
        "Medium": "orange",
        "Low": "yellow",
    }

    for t in tickets:
        with st.expander(f"#{t['id']} — {t['subject']} ({t['status']})"):
            if st.button(f"Delete Ticket #{t['id']}", key=f"close_{t['id']}"):
                store.delete_ticket(t["id"])
                st.toast(f"Ticket #{t['id']} deleted.")
                st.rerun()

            priority_color = priority_colors.get(t["priority"], "#6b7280")

            tag_1_html = (
                f'<span style="background-color: #374151; color: #ffffff; padding: 3px 10px; border-radius: 12px; font-size: 0.8em;">{t["tag_1"]}</span>'
                if t.get("tag_1")
                else ""
            )
            tag_2_html = (
                f'<span style="background-color: #374151; color: #ffffff; padding: 3px 10px; border-radius: 12px; font-size: 0.8em;">{t["tag_2"]}</span>'
                if t.get("tag_2")
                else ""
            )

            st.markdown(
                f"""
                <div style="display: flex; flex-wrap: wrap; gap: 12px; align-items: center; font-size: 0.9em; margin-bottom: 15px; margin-top: 5px;">
                    <span><b>Priority:</b> <span style="color: {priority_color}; font-weight: 600;">{t['priority']}</span></span>
                    <span><b>Type:</b> <span style="color: #fb923c;">{t['type']}</span></span>
                    <span><b>Category:</b> <span style="color: #4ade80;">{t['category']}</span></span>
                    <span style="color: #4b5563;">|</span>
                    {tag_1_html}
                    {tag_2_html}
                </div>
                """,
                unsafe_allow_html=True,
            )

            st.markdown("---")
            st.write(f"**Description:**")
            st.info(t["body"])

            st.write(f"**AI Initial Response:**")
            st.warning(t["answer"])
