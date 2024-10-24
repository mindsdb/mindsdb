from fastapi import FastAPI, HTTPException
from pymilvus import connections, Collection, utility, FieldSchema, CollectionSchema, DataType
from pydantic import BaseModel
import os

app = FastAPI()

# Milvus connection details
MILVUS_HOST = os.getenv("MILVUS_HOST", "standalone")
MILVUS_PORT = os.getenv("MILVUS_PORT", 19530)


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
async def search_collection(collection_name: str, query_vector: list[float]):
    if not utility.has_collection(collection_name):
        raise HTTPException(status_code=404, detail="Collection not found")

    collection = Collection(collection_name)
    collection.load()

    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
    results = collection.search(
        data=[query_vector],
        anns_field="vector_field",  # replace with your vector field name
        param=search_params,
        limit=5,
        expr=None
    )

    return {"results": results[0].distances, "ids": results[0].ids}


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


@app.post("/collection")
async def create_collection(name: str, dimension: int):
    if utility.has_collection(name):
        raise HTTPException(status_code=400, detail="Collection already exists")

    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=40000),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=40000),
        FieldSchema(name="published_at", dtype=DataType.VARCHAR, max_length=40000),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=40000),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dimension)
    ]
    schema = CollectionSchema(fields, "A collection for storing vector embeddings")
    _ = Collection(name, schema)

    return {"message": f"Collection '{name}' created successfully", "schema": schema.to_dict()}

# Add more endpoints as needed for your specific use case
