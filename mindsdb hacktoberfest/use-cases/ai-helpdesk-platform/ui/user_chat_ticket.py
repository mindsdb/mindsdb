import streamlit as st
from core.json_store import JsonStore
from core.agent_ops import get_ai_response

store = JsonStore()


def run():
    st.title("💬 Chat with AI")

    tickets = store.list_tickets()
    if not tickets:
        st.info("No open tickets found.")
        return

    ticket_options = {f"#{t['id']} - {t['subject']}": t["id"] for t in tickets}
    selected_option = st.selectbox("Select a Ticket:", options=ticket_options.keys())
    ticket_id = ticket_options[selected_option]

    if (
        "current_ticket_id" not in st.session_state
        or st.session_state.current_ticket_id != ticket_id
    ):
        st.session_state.current_ticket_id = ticket_id
        ticket = store.get_ticket(ticket_id)
        st.session_state.messages = (
            ticket.get(  # pyright: ignore[reportOptionalMemberAccess]
                "conversation", []
            )
        )
        st.rerun()

    ticket = store.get_ticket(ticket_id)
    st.markdown(f"### {ticket['subject']}")  # pyright: ignore[reportOptionalSubscript]
    st.caption(
        f"Priority: {ticket['priority']} | Category: {ticket['category']}"  # pyright: ignore[reportOptionalSubscript]
    )
    st.divider()

    for msg in st.session_state.get("messages", []):
        with st.chat_message(msg["sender"]):
            st.markdown(msg["message"])

    is_chat_disabled = ticket.get("status") != "open" # pyright: ignore[reportOptionalMemberAccess]
    placeholder_text = (
        "This ticket is closed. You can no longer send messages."
        if is_chat_disabled
        else "Type your message to AI..."
    )

    if user_msg := st.chat_input(placeholder_text, disabled=is_chat_disabled):
        st.session_state.messages.append({"sender": "user", "message": user_msg})
        store.append_message(ticket_id, "user", user_msg)

        with st.chat_message("user"):
            st.markdown(user_msg)

        with st.spinner("AI is thinking..."):
            history_for_agent = []
            conversation = st.session_state.messages
            for i in range(0, len(conversation) - 1, 2):
                if (
                    conversation[i]["sender"] == "user"
                    and conversation[i + 1]["sender"] == "ai"
                ):
                    history_for_agent.append(
                        {
                            "question": conversation[i]["message"],
                            "answer": conversation[i + 1]["message"],
                        }
                    )

            ai_reply = get_ai_response(user_msg, message_history=history_for_agent)

            ai_entry = {"sender": "ai", "message": ai_reply}
            st.session_state.messages.append(ai_entry)
            store.append_message(ticket_id, "ai", ai_reply)

        st.rerun()
