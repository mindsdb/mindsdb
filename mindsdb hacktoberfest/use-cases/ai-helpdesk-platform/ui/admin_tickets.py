import streamlit as st
from core.json_store import JsonStore
from core.kb_ops import add_ticket_to_kb

store = JsonStore()


def run():
    st.subheader("🗂 Manage Tickets")

    all_tickets = store.list_tickets()
    tickets = [ticket for ticket in all_tickets if ticket.get("status") == "open"]

    if not tickets:
        st.info("No tickets raised yet.")
        return

    # Grid layout
    cols_per_row = 3
    for i in range(0, len(tickets), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, ticket in enumerate(tickets[i : i + cols_per_row]):
            col = cols[j]
            with col:
                # Ticket card
                st.markdown(f"### {ticket['subject']}")
                body_preview = ticket["body"][:100] + (
                    "..." if len(ticket["body"]) > 100 else ""
                )
                st.write(body_preview)

                # Metadata badges
                st.markdown(
                    f"<span style='background-color:#FFD700;padding:2px 6px;border-radius:4px;color:#000;'>Priority: {ticket['priority']}</span> "
                    f"<span style='background-color:#87CEFA;padding:2px 6px;border-radius:4px;color:#000;'>Type: {ticket['type']}</span> "
                    f"<span style='background-color:#90EE90;padding:2px 6px;border-radius:4px;color:#000;'>Category: {ticket['category']}</span>",
                    unsafe_allow_html=True,
                )

                # Tags
                tags_html = ""
                for tag in [ticket.get("tag_1"), ticket.get("tag_2")]:
                    if tag:
                        tags_html += f"<span style='background-color:#D8BFD8;padding:2px 6px;margin-right:3px;border-radius:4px;color:#000;'>{tag}</span>"
                st.markdown(tags_html, unsafe_allow_html=True)

                with st.expander("View Full Body"):
                    st.write(f"**Subject:** {ticket['subject']}")
                    st.write(f"**Body:** {ticket['body']}")

                    col1, col2 = st.columns(2)

                    with col1:
                        if st.button(
                            f"Add to KB #{ticket['id']}", key=f"kb_{ticket['id']}"
                        ):
                            with st.spinner(
                                "Processing and adding to Knowledge Base..."
                            ):
                                id_ = add_ticket_to_kb(ticket)
                                store.update_status(ticket["id"], "closed")
                            st.success(
                                f"✅ Added to KB successfully. Document ID: `{id_}`"
                            )

                    with col2:
                        if st.button(
                            f"Delete #{ticket['id']}", key=f"del_{ticket['id']}"
                        ):
                            store.delete_ticket(ticket["id"])
                            st.success("Deleted.")
