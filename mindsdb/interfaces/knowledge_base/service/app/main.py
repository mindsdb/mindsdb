from fastapi import FastAPI, HTTPException
from pymilvus import connections, Collection, utility, FieldSchema, CollectionSchema, DataType
from pydantic import BaseModel
import os
import aiohttp

app = FastAPI()

# Milvus connection details
MILVUS_HOST = os.getenv("MILVUS_HOST", "standalone")
MILVUS_PORT = os.getenv("MILVUS_PORT", 19530)
EMBEDDING_HOST = "ollama"
EMBEDDING_PORT = 11434
OLLAMA_EMBEDDING_URL = f"http://{EMBEDDING_HOST}:{EMBEDDING_PORT}/api/embeddings"
HNSW_DEFAULT_EF_SEARCH = 20
HNSW_DEFAULT_EF_CONSTRUCTION = 40
HNSW_DEFAULT_M = 1024
MAX_TEXT_LENGTH = 40000


# curl command example:
# curl http://ollama:11434/api/embeddings -d '{
#   "model": "nomic-embed-text",
#   "prompt": "The sky is blue because of Rayleigh scattering"
# }'

class IngestibleText(BaseModel):
    id: str
    title: str
    published_at: str
    text: str


# Connect to Milvus
@app.on_event("startup")
async def startup_event():
    connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
    print(f"Connected to Milvus server at {MILVUS_HOST}:{MILVUS_PORT}")


@app.on_event("shutdown")
async def shutdown_event():
    connections.disconnect("default")
    print("Disconnected from Milvus server")


@app.get("/")
async def root():
    return {"message": "Welcome to the Milvus FastAPI service"}


@app.get("/collections")
async def list_collections():
    return {"collections": utility.list_collections()}


@app.get("/collection/{name}")
async def get_collection_info(name: str):
    if utility.has_collection(name):
        collection = Collection(name)
        return {
            "name": name,
            "schema": collection.schema,
            "num_entities": collection.num_entities
        }
    raise HTTPException(status_code=404, detail="Collection not found")


@app.post("/search/{collection_name}")
async def search_collection(collection_name: str,
                            query_vector: list[float],
                            limit: int = 5):
    if not utility.has_collection(collection_name):
        raise HTTPException(status_code=404, detail="Collection not found")

    collection = Collection(collection_name)
    collection.load()

    search_params = {"metric_type": "COSINE", "params": {"ef": HNSW_DEFAULT_EF_SEARCH}}
    results = collection.search(
        data=[query_vector],
        anns_field="embedding",  # replace with your vector field name
        param=search_params,
        limit=limit,
        expr=None
    )

    return {"results": results[0].distances, "ids": results[0].ids}


# Fetch embeddings from Ollama service and then search in Milvus
@app.post("/search_text/{collection_name}")
async def search_text(collection_name: str, text: str):
    embedding = await fetch_embeddings(text)
    return await search_collection(collection_name, embedding)


async def fetch_embeddings(text: str) -> list[float]:
    url = OLLAMA_EMBEDDING_URL
    payload = {
        "model": "nomic-embed-text",
        "prompt": text
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            response_data = await response.json()
    return response_data["embedding"]


@app.post("/embed")
async def embed_text(text: str):
    embedding = await fetch_embeddings(text)
    return {"embedding": embedding}


@app.post("/insert/{collection_name}")
async def insert_vector(collection_name: str, text: IngestibleText, embedding: list[float]):
    if not utility.has_collection(collection_name):
        raise HTTPException(status_code=404, detail="Collection not found")

    collection = Collection(collection_name)
    dictionary_to_insert = {"id": text.id,
                            "title": text.title,
                            "published_at": text.published_at,
                            "text": text.text,
                            "embedding": embedding}
    collection.insert([dictionary_to_insert])
    collection.load()
    return {"message": "Text ingested successfully"}


# endpoint to embed and then insert text
@app.post("/ingest/{collection_name}")
async def ingest_text(collection_name: str, text: IngestibleText):
    if not utility.has_collection(collection_name):
        raise HTTPException(status_code=404, detail="Collection not found")

    embedding = await fetch_embeddings(text.text)
    return await insert_vector(collection_name, text, embedding)


@app.post("/collection")
async def create_collection(name: str, dimension: int):
    if utility.has_collection(name):
        raise HTTPException(status_code=400, detail="Collection already exists")

    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=MAX_TEXT_LENGTH),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=MAX_TEXT_LENGTH),
        FieldSchema(name="published_at", dtype=DataType.VARCHAR, max_length=MAX_TEXT_LENGTH),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=MAX_TEXT_LENGTH),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dimension)
    ]
    schema = CollectionSchema(fields, "A collection for storing vector embeddings")
    collection = Collection(name, schema)
    # add index
    # Ensure the collection and field name are correct
    field_name = "embedding"  # This should match the field defined in your collection schema

    index_params = {
        "index_type": "HNSW",  # Or another suitable type depending on your needs and the size of your dataset
        "metric_type": "COSINE",
        'params': {'efConstruction': HNSW_DEFAULT_EF_CONSTRUCTION, 'M': HNSW_DEFAULT_M}
    }

    # # Create the index
    collection.create_index(field_name=field_name, index_params=index_params)

    return {"message": f"Collection '{name}' created successfully", "schema": schema.to_dict()}

# Add more endpoints as needed for your specific use case
