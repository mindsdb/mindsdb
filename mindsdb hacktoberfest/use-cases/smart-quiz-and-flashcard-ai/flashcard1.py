import mindsdb_sdk as mdb
import json

server = mdb.connect()
files = server.get_database("files")


def generate_flashcard(Query: str):
    agent = server.agents.get("Flash_card_Agent")
    response = agent.completion([{"question": Query, "answer": None}])
    return json.loads(response.content)
