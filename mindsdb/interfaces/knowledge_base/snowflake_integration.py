import requests


def embedding(self, model_name: str, project_id: str, pat_token: str, texts: list[str]):
    url = f"https://{project_id}.snowflakecomputing.com/api/v2/cortex/inference:embed"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {pat_token}",
        "content-type": "application/json",
    }
    payload = {"text": texts, "model": model_name}
    response = requests.post(url, headers=headers, json=payload)
    embeddings = []

    response_json = response.json()
    for elem in response_json.get("data", []):
        emb = elem.get("embedding")
        embeddings.append(emb)
    return embeddings
