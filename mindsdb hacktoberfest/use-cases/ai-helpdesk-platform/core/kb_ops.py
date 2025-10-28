import mindsdb_sdk as mdb
import pandas as pd

server = mdb.connect()
files = server.get_database("files")


def get_max_id():
    query = "SELECT MAX(id) as max_id FROM files.tickets"
    result = server.query(query).fetch()
    return result["max_id"][0]


def add_ticket_to_kb(ticket):
    max_id = get_max_id()
    new_id = max_id + 1 if max_id is not None else 0

    tickets_table = files.get_table("tickets")
    tickets_table.insert(
        pd.DataFrame(
            {
                "id": [new_id],
                "subject": [ticket["subject"]],
                "body": [ticket["body"]],
                "category": [ticket["category"]],
                "priority": [ticket["priority"]],
                "type": [ticket["type"]],
                "tag_1": [ticket["tag_1"]],
                "tag_2": [ticket["tag_2"]],
                "answer": [ticket["conversation"][0]["message"]],
            }
        )
    )

    return new_id


def search_kb(
    content: str,
    filters: dict = None,  # pyright: ignore[reportArgumentType]
    top_k: int = 3,
):
    if filters:
        conditions = [f"content = '{content}'"]

        for key in ["type", "priority", "category", "tag_1", "tag_2"]:
            if filters.get(key) and filters[key] != "Any":
                conditions.append(f"{key} = '{filters[key]}'")

        where_clause = " AND ".join(conditions)

        query = f"SELECT * FROM tickets_kb WHERE {where_clause}"
    else:
        query = f"SELECT * FROM tickets_kb WHERE content = '{content}'"

    results = server.query(query).fetch()[:top_k]
    return results


def get_tickets_dataset():
    return files.get_table("tickets").fetch()