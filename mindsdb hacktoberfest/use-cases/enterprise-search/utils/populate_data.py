import json
import mindsdb_sdk

def load_data(data_source):
    with open(f"test_data/{data_source}.json", 'r', encoding='utf-8') as f:
        data = json.load(f)

    server = mindsdb_sdk.connect()
    kb = server.knowledge_bases.get(f"{data_source}_kb")
    kb.insert(data)
    

def main():
    load_data("jira")
    load_data("zendesk")