import json
import mindsdb_sdk as mdb

server = mdb.connect()

def classify_ticket(subject: str, body: str):
    text = f"Subject: {subject}\nBody: {body}"

    agent = server.agents.get("ticket_classifier")
    response = agent.completion([{"question": text, "answer": None}])
    return json.loads(response.content)


def get_ai_response(
    message: str,
    message_history: list[dict] = None,  # pyright: ignore[reportArgumentType]
):
    agent = server.agents.get("support_agent")

    if message_history:
        response = agent.completion(
            messages=message_history + [{"question": message, "answer": None}]
        )
    else:
        response = agent.completion([{"question": message, "answer": None}])
    return response.content
