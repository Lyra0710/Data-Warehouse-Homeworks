from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pinecone import Pinecone, ServerlessSpec
from airflow.models import Variable
from sentence_transformers import SentenceTransformer
import ast 


import os
import pandas as pd
import requests
INDEX_NAME = "semantic-search-fast"

DATA_DIR = "/opt/airflow/data"
RAW_CSV = os.path.join(DATA_DIR, "medium_data.csv")
PROCESSED_CSV = os.path.join(DATA_DIR, "medium_input.csv")

MEDIUM_DATA_URL = "https://s3-geospatial.s3.us-west-2.amazonaws.com/medium_data.csv"

def download_process_generate_input():
    os.makedirs(DATA_DIR, exist_ok=True)

    r = requests.get(MEDIUM_DATA_URL, timeout=60)
    r.raise_for_status()
    with open(RAW_CSV, "wb") as f:
        f.write(r.content)

    df = pd.read_csv(RAW_CSV)

    df["title"] = df["title"].astype(str).fillna("")
    df["subtitle"] = df["subtitle"].astype(str).fillna("")

    df["metadata"] = df.apply(
        lambda row: {
            "title": (row["title"] + " " + row["subtitle"]).strip()
        },
        axis=1
    )

    df_input = df[["id", "metadata"]].copy()
    df_input["id"] = df_input["id"].astype(str)

    df_input = df_input[
        df_input["metadata"].apply(lambda x: bool(x["title"].strip()))
    ].copy()

    df_input.to_csv(PROCESSED_CSV, index=False)

    print(f"Downloaded raw file to: {RAW_CSV}")
    print(f"Generated processed input file to: {PROCESSED_CSV}")


def create_pinecone_index():
    api_key = Variable.get("pinecone_api_key")

    pc = Pinecone(api_key=api_key)

    existing_indexes = pc.list_indexes().names()

    if INDEX_NAME in existing_indexes:
        print(f"Index '{INDEX_NAME}' already exists.")
        return

    pc.create_index(
        name=INDEX_NAME,
        dimension=384,
        metric="dotproduct",
        spec=ServerlessSpec(
            cloud="aws",
            region="us-east-1"
        )
    )

    print(f"Created Pinecone index: {INDEX_NAME}")

def embed_and_ingest():
    api_key = Variable.get("pinecone_api_key")

    # Load processed input from step 3
    df = pd.read_csv(PROCESSED_CSV)

    # metadata may come back from CSV as a string, so convert it back to dict
    df["metadata"] = df["metadata"].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )

    # Load embedding model
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Step 5: generate embeddings from metadata["title"]
    df["values"] = df["metadata"].apply(
        lambda x: model.encode(x["title"]).tolist()
    )

    # Step 6: build ingestion dataframe
    df_upsert = df[["id", "values", "metadata"]].copy()
    df_upsert["id"] = df_upsert["id"].astype(str)

    # Step 7: ingest into Pinecone
    pc = Pinecone(api_key=api_key)
    index = pc.Index(INDEX_NAME)
    index.upsert_from_dataframe(df_upsert)

    print(f"Embedded {len(df_upsert)} rows")
    print(f"Upserted {len(df_upsert)} rows into Pinecone index '{INDEX_NAME}'")
    print(df_upsert.head().to_dict(orient="records"))

def query_pinecone():
    api_key = Variable.get("pinecone_api_key")

    pc = Pinecone(api_key=api_key)
    index = pc.Index("semantic-search-fast")

    model = SentenceTransformer("all-MiniLM-L6-v2")

    query_text = "what is machine learning"
    query_vector = model.encode(query_text).tolist()

    results = index.query(
        vector=query_vector,
        top_k=5,
        include_metadata=True
    )

    print("Query:", query_text)
    print("Results:")

    for match in results["matches"]:
        print({
            "id": match["id"],
            "score": match["score"],
            "text": match["metadata"].get("title")
        })

with DAG(
    dag_id="pinecone_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    step3_task = PythonOperator(
        task_id="download_process_generate_input",
        python_callable=download_process_generate_input,
    )

    create_index_task = PythonOperator(
    task_id="create_pinecone_index",
    python_callable=create_pinecone_index,
)

    embed_ingest_task = PythonOperator(
    task_id="embed_and_ingest",
    python_callable=embed_and_ingest,
)
    query_task = PythonOperator(
    task_id="query_pinecone",
    python_callable=query_pinecone,
)
    step3_task >> create_index_task >> embed_ingest_task >> query_task