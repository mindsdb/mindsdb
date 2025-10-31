import json, os, datetime
from typing import Dict, List

DATA_PATH = "data/data.json"


class JsonStore:
    def __init__(self, path=DATA_PATH):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            with open(path, "w") as f:
                json.dump({"tickets": []}, f, indent=2)

    def _read(self):
        with open(self.path, "r") as f:
            return json.load(f)

    def _write(self, data):
        with open(self.path, "w") as f:
            json.dump(data, f, indent=2)

    def create_ticket(self, ticket: Dict) -> int:
        data = self._read()
        ticket_id = max([t["id"] for t in data["tickets"]] + [0]) + 1
        ticket["id"] = ticket_id
        ticket["created_at"] = datetime.datetime.utcnow().isoformat()
        ticket["conversation"] = []
        data["tickets"].append(ticket)
        self._write(data)
        return ticket_id

    def append_message(self, ticket_id: int, sender: str, message: str):
        data = self._read()
        for t in data["tickets"]:
            if t["id"] == ticket_id:
                t["conversation"].append(
                    {
                        "sender": sender,
                        "message": message,
                        "ts": datetime.datetime.utcnow().isoformat(),
                    }
                )
        self._write(data)

    def list_tickets(self) -> List[Dict]:
        return self._read()["tickets"]

    def get_ticket(self, ticket_id: int):
        return next((t for t in self._read()["tickets"] if t["id"] == ticket_id), None)

    def update_status(self, ticket_id: int, status: str):
        data = self._read()
        for t in data["tickets"]:
            if t["id"] == ticket_id:
                t["status"] = status
        self._write(data)

    def delete_ticket(self, ticket_id: int):
        data = self._read()
        for i, t in enumerate(data["tickets"]):
            if t["id"] == ticket_id:
                data["tickets"].pop(i)
        self._write(data)
