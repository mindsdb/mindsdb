import streamlit as st
from core.json_store import JsonStore
from core.agent_ops import classify_ticket, get_ai_response

store = JsonStore()

def run():
    st.subheader("Submit New Ticket")

    subject = st.text_input("Subject")
    body = st.text_area("Describe your issue", height=150)

    if st.button("Submit"):
        with st.status("Submitting ticket, please wait...", expanded=True) as status:
            st.write("🧠 Classifying ticket...")
            meta = classify_ticket(subject, body)

            st.write("🤖 Generating initial AI response...")
            message = f"Subject: {subject}\nBody: {body}"
            ai_reply = get_ai_response(message)

            ticket = {
                "subject": subject,
                "body": body,
                "answer": ai_reply,
                **meta,
                "status": "open",
            }

            st.write("💾 Saving ticket to database...")
            ticket_id = store.create_ticket(ticket)
            store.append_message(ticket_id, "ai", ai_reply)
            
            status.update(label="✅ Ticket processed!", state="complete", expanded=False)

        st.success(f"Ticket #{ticket_id} submitted successfully.")
        st.info(f"**AI Response:**\n\n{ai_reply}")
