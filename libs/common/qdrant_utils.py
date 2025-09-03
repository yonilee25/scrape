from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from libs.common.config import Settings
import uuid

settings = Settings()
client = QdrantClient(url=settings.VECTOR_URL)

COLLECTION = "chunks"

def ensure_collection():
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION not in collections:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=settings.EMBEDDING_DIM, distance=Distance.COSINE),
        )

def upsert_chunk(vector, payload: dict):
    ensure_collection()
    client.upsert(
        collection_name=COLLECTION,
        points=[PointStruct(id=str(uuid.uuid4()), vector=vector, payload=payload)]
    )

def search_similar(vector, job_id: str, limit: int = 5):
    ensure_collection()
    scroll_filter = Filter(
        must=[FieldCondition(key="job_id", match=MatchValue(value=job_id))]
    )
    return client.search(collection_name=COLLECTION, query_vector=vector, limit=limit, query_filter=scroll_filter)
