import json
from typing import List, Dict
from pymilvus import MilvusClient, DataType
from embed import get_embedding

# 1. 设置一个Milvus客户端
milvus_client = MilvusClient(
    uri="http://localhost:19530"
)

COLLECTION_NAME = "code_documentation"
EMBEDDING_DIMENSION = 1536  # for text-embedding-3-small


def create_milvus_collection():
    if milvus_client.has_collection(collection_name=COLLECTION_NAME):
        milvus_client.drop_collection(collection_name=COLLECTION_NAME)

    milvus_client.create_collection(
        collection_name=COLLECTION_NAME,
        dimension=EMBEDDING_DIMENSION,
    )


def insert_documents(documents: List[Dict]):
    data = []
    for doc in documents:
        vector = get_embedding(doc['documentation'])
        data.append({
            "id": len(data),
            "vector": vector,
            "file_path": doc['file_path'],
            "entity_name": doc['entity_name'],
            "entity_type": doc['entity_type'],
            "vector_source": doc['documentation']
        })

        vector1 = get_embedding(doc['code'])
        data.append({
            "id": len(data),
            "vector": vector1,
            "file_path": doc['file_path'],
            "entity_name": doc['entity_name'],
            "entity_type": doc['entity_type'],
            "vector_source": doc['code']
        })

    milvus_client.insert(collection_name=COLLECTION_NAME, data=data)


def load_jsonl(file_path: str) -> List[Dict]:
    with open(file_path, 'r') as file:
        return [json.loads(line) for line in file]


def search_similar_documents(query: str, limit: int = 3):
    query_vector = get_embedding(query)
    results = milvus_client.search(
        collection_name=COLLECTION_NAME,
        data=[query_vector],
        limit=limit,
        output_fields=["file_path", "entity_name", "entity_type", "documentation"]
    )
    return results


if __name__ == "__main__":
    #create_milvus_collection()
    #rst = load_jsonl("eduplatform-backend-1.jsonl")
    #insert_documents(rst)
    similar_docs = search_similar_documents("判断上传文件类型并调用算法接口抽取")
