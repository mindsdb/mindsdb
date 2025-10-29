import streamlit as st
import json
import os
from flashcard1 import generate_flashcard
from quiz1 import generate_quiz

# =========================================================
#  Helper Functions
# =========================================================
DATA_FILE = "learning_data.json"


def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}
    data.setdefault("flashcards", {})
    data.setdefault("quizzes", {})
    return data


def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)


# =========================================================
#  Flashcard Display
# =========================================================
def show_flashcard(topic, cards):
    import streamlit.components.v1 as components

    html = ""
    for i, card in enumerate(cards):
        question = card.get("question", "")
        answer = card.get("answer", "")
        html += f"""
        <div class="flip-card" style="margin-bottom: 20px;">
          <div class="flip-card-inner">
            <div class="flip-card-front">
              <h4>Q{i+1}: {question}</h4>
            </div>
            <div class="flip-card-back">
              <p><b>Answer:</b> {answer}</p>
            </div>
          </div>
        </div>
        """
    components.html(
        f"""
        <style>
        .flip-card {{
            background-color: transparent;
            width: 100%;
            height: 150px;
            perspective: 1000px;
        }}
        .flip-card-inner {{
            position: relative;
            width: 100%;
            height: 100%;
            text-align: center;
            transition: transform 0.8s;
            transform-style: preserve-3d;
            cursor: pointer;
        }}
        .flip-card:hover .flip-card-inner {{
            transform: rotateY(180deg);
        }}
        .flip-card-front, .flip-card-back {{
            position: absolute;
            width: 100%;
            height: 100%;
            backface-visibility: hidden;
            border: 2px solid #ccc;
            border-radius: 10px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            padding: 20px;
        }}
        .flip-card-front {{
            background: linear-gradient(135deg, #84fab0, #8fd3f4);
            color: black;
        }}
        .flip-card-back {{
            background: linear-gradient(135deg, #89f7fe, #66a6ff);
            color: black;
            transform: rotateY(180deg);
        }}
        </style>
        {html}
        """,
        height=700,
    )


# =========================================================
# Quiz Display
# =========================================================
def show_quiz(topic, questions):
    st.subheader(f"📝 Quiz: {topic}")
    score = 0

    for i, q in enumerate(questions):
        question = q.get("question", "Untitled question")
        correct_answer = q.get("answer", "").strip().lower()
        options = q.get("options", [])

        st.markdown(f"### Q{i+1}: {question}")

        if options and isinstance(options, list):
            user_choice = st.radio(
                "Select your answer:",
                options,
                key=f"quiz_{topic}_{i}",
                index=None,
            )
            if user_choice:
                if user_choice.strip().lower() == correct_answer:
                    st.success("✅ Correct!")
                    score += 1
                else:
                    st.error(f"❌ Incorrect! Correct answer: **{q['answer']}**")
        else:
            user_answer = st.text_input("Your answer:", key=f"quiz_text_{topic}_{i}")
            if user_answer:
                if user_answer.strip().lower() == correct_answer:
                    st.success("✅ Correct!")
                    score += 1
                else:
                    st.error(f"❌ Incorrect! Correct answer: **{q['answer']}**")

        st.markdown("---")

    st.info(f"**Your Score: {score}/{len(questions)}**")
    if score == len(questions):
        st.balloons()


st.markdown(
    """
    <style>
    .main-title {
        text-align: center;
        font-size: 2.5rem;
        font-weight: bold;
        background: -webkit-linear-gradient(45deg, #36D1DC, #5B86E5);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .stButton button {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
        border: none;
        padding: 0.5em 1em;
        border-radius: 10px;
        transition: 0.3s ease-in-out;
    }
    .stButton button:hover {
        transform: scale(1.05);
        background: linear-gradient(45deg, #6a11cb, #2575fc);
    }
    </style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    '<p class="main-title">📘 AI Flashcards & Quiz Generator</p>',
    unsafe_allow_html=True,
)
st.markdown("---")

# Sidebar
page = st.sidebar.radio(
    "📂 Navigate", ["🏠 Home", "🧠 Flashcards", "📝 Quizzes", "📦 View Stored Data"]
)

data = load_data()


# =========================================================
#  HOME PAGE
# =========================================================
if page == "🏠 Home":
    st.markdown(
        """
        <style>
            .main-title {
                font-size: 3.5rem;
                text-align: center;
                font-weight: 800;
                background: linear-gradient(90deg, #4facfe, #00f2fe);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-top: 60px;
                animation: fadeIn 2s ease-in-out;
            }
            .subtitle {
                text-align: center;
                font-size: 1.2rem;
                color: #b0b0b0;
                margin-bottom: 40px;
            }
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(-10px); }
                to { opacity: 1; transform: translateY(0); }
            }
        </style>
    """,
        unsafe_allow_html=True,
    )

    st.markdown(
        "<div class='main-title'>AI Flashcards & Quiz Generator</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='subtitle'>Learn faster with AI-powered flashcards and quizzes 🚀</div>",
        unsafe_allow_html=True,
    )

    st.markdown("### 🌟 Features")
    st.info(
        """
    - AI-generated flashcards and quizzes  
    - Animated flip cards for better engagement  
    - Topic management (Add / Delete / View)  
    - Local storage so your progress is saved
    """
    )

    st.markdown("### 🚀 Get Started")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🧠 Go to Flashcards"):
            st.session_state["page"] = "🧠 Flashcards"
            st.experimental_rerun()

    with col2:
        if st.button("📝 Go to Quizzes"):
            st.session_state["page"] = "📝 Quizzes"
            st.experimental_rerun()

# =========================================================
#  FLASHCARDS
# =========================================================
elif page == "🧠 Flashcards":
    st.header("✨ Flashcard Generator")
    topic = st.text_input("Enter a topic for Flashcards:")
    if st.button("Generate Flashcards"):
        if topic:
            with st.spinner("Generating flashcards..."):
                flashcards = generate_flashcard(topic)
                if flashcards:
                    data["flashcards"][topic] = flashcards
                    save_data(data)
                    st.success(f"✅ Flashcards for '{topic}' generated!")
                    show_flashcard(topic, flashcards)
                else:
                    st.error("⚠️ Could not generate flashcards. Try again later.")
        else:
            st.warning("Please enter a topic first.")

    if data["flashcards"]:
        st.markdown("### 📚 Stored Flashcards")
        topic_list = ["None"] + list(data["flashcards"].keys())
        selected_topic = st.selectbox("Choose a topic:", topic_list)

        if selected_topic != "None":
            show_flashcard(selected_topic, data["flashcards"][selected_topic])
            if st.button(f"🗑️ Delete '{selected_topic}' Flashcards"):
                del data["flashcards"][selected_topic]
                save_data(data)
                st.success(f"Deleted flashcards for '{selected_topic}'.")
                st.rerun()

# =========================================================
# QUIZZES
# =========================================================
elif page == "📝 Quizzes":
    st.header("🧩 Quiz Generator")
    topic = st.text_input("Enter a topic for Quiz:")
    if st.button("Generate Quiz"):
        if topic:
            with st.spinner("Generating quiz questions..."):
                quiz_data = generate_quiz(topic)
                if quiz_data:
                    data["quizzes"][topic] = quiz_data
                    save_data(data)
                    st.success(f"✅ Quiz for '{topic}' generated successfully!")
                    show_quiz(topic, quiz_data)
                else:
                    st.error("⚠️ Could not generate quiz. Try again later.")
        else:
            st.warning("Please enter a topic first.")

    if data["quizzes"]:
        st.markdown("### 🗂️ Stored Quizzes")
        topic_list = ["None"] + list(data["quizzes"].keys())
        selected_topic = st.selectbox("Choose a topic:", topic_list)

        if selected_topic != "None":
            show_quiz(selected_topic, data["quizzes"][selected_topic])
            if st.button(f"🗑️ Delete '{selected_topic}' Quiz"):
                del data["quizzes"][selected_topic]
                save_data(data)
                st.success(f"Deleted quiz for '{selected_topic}'.")
                st.rerun()

# =========================================================
#  DATA VIEW
# =========================================================
elif page == "📦 View Stored Data":
    st.header("📂 Stored Flashcards & Quizzes")
    if not data["flashcards"] and not data["quizzes"]:
        st.info("No data stored yet.")
    else:
        with st.expander("🧠 Flashcards"):
            st.json(data["flashcards"])
        with st.expander("📝 Quizzes"):
            st.json(data["quizzes"])

    if st.button("🗑️ Clear All Data"):
        os.remove(DATA_FILE)
        st.success("All stored data cleared!")
        st.rerun()
