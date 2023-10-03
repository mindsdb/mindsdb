import requests

class NotionAPIHandler:
    def __init__(self, api_token):
        self.api_token = api_token

    def connect(self):
        pass

    def disconnect(self):
        pass

    def query(self, query):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        response = requests.post("https://api.notion.com/v1/search", headers=headers, json=query)
        results = response.json()["results"]
        return results

    def mutate(self, mutation):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        response = requests.post("https://api.notion.com/v1/pages", headers=headers, json=mutation)
        if response.status_code != 200:
            raise Exception("Failed to execute Notion API mutation.")

    def get_page(self, page_id):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        response = requests.get(f"https://api.notion.com/v1/pages/{page_id}", headers=headers)
        page = response.json()
        return page

    def update_page(self, page_id, page_data):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        response = requests.put(f"https://api.notion.com/v1/pages/{page_id}", headers=headers, json=page_data)
        if response.status_code != 200:
            raise Exception("Failed to update Notion page.")

    def delete_page(self, page_id):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        response = requests.delete(f"https://api.notion.com/v1/pages/{page_id}", headers=headers)
        if response.status_code != 200:
            raise Exception("Failed to delete Notion page.")

    def create_database(self, database_name, database_type):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        payload = {"name": database_name, "type": database_type}
        response = requests.post("https://api.notion.com/v1/databases", headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception("Failed to create Notion database.")

        database = response.json()
        return database

    def create_table(self, database_id, table_name, table_properties):
        headers = {"Authorization": f"Bearer {self.api_token}"}
        payload = {"name": table_name, "properties": table_properties}
        response = requests.post(f"https://api.notion.com/v1/databases/{database_id}/tables", headers=headers, json=payload)
        if response.status_code != 200:
            raise Exception("Failed to create Notion table.")

        table = response.json()
        return table
