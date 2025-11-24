import os
from datetime import datetime
from typing import List

import mysql.connector
from dotenv import load_dotenv
from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery


# -------------------- LOAD ENV --------------------
load_dotenv()

# MySQL env
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# Azure OpenAI env
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")
AZURE_OPENAI_EMBED_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMBED_DEPLOYMENT")

# Azure Search env
AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
AZURE_SEARCH_KEY = os.getenv("AZURE_SEARCH_KEY")
AZURE_SEARCH_INDEX = os.getenv("AZURE_SEARCH_INDEX")


# -------------------- CLIENTS --------------------

# MySQL client
def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )


# Azure OpenAI client
aoai = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_OPENAI_API_VERSION
)

# Azure Search client (FIXED)
search_client = SearchClient(
    endpoint=AZURE_SEARCH_ENDPOINT,
    index_name=AZURE_SEARCH_INDEX,
    credential=AzureKeyCredential(AZURE_SEARCH_KEY)
)


# -------------------- EMBEDDING FUNCTION --------------------

def get_embedding(text: str) -> List[float]:
    res = aoai.embeddings.create(
        model=AZURE_OPENAI_EMBED_DEPLOYMENT,
        input=text
    )
    return res.data[0].embedding


# -------------------- METADATA EXTRACTION --------------------

def fetch_metadata(conn):
    sql = """
        SELECT table_schema, table_name, column_name, data_type,
               is_nullable, IFNULL(column_comment, '') AS column_comment
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position;
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (MYSQL_DB,))
    rows = cur.fetchall()
    cur.close()
    return rows


def profile_column(conn, table, column):
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    total = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE `{column}` IS NULL")
    nulls = cur.fetchone()[0]
    null_ratio = nulls / total if total > 0 else 0

    cur.execute(f"SELECT COUNT(DISTINCT `{column}`) FROM `{table}`")
    distinct = cur.fetchone()[0]

    cur.execute(f"SELECT `{column}` FROM `{table}` WHERE `{column}` IS NOT NULL LIMIT 5")
    samples = [str(r[0]) for r in cur.fetchall()]

    cur.close()

    return {
        "null_ratio": null_ratio,
        "approx_distinct": distinct,
        "sample_values": samples
    }


def build_text_blob(meta, prof):
    nullable = "nullable" if meta["is_nullable"] == "YES" else "not nullable"

    blob = (
        f"Column '{meta['column_name']}' in table '{meta['table_name']}' of database '{meta['table_schema']}'. "
        f"Datatype: {meta['data_type']}. It is {nullable}. "
        f"Comment: {meta['column_comment']}. "
        f"Null ratio: {prof['null_ratio']:.2f}. "
        f"Approx distinct: {prof['approx_distinct']}. "
        f"Samples: {', '.join(prof['sample_values']) if prof['sample_values'] else 'none'}."
    )
    return blob


# -------------------- BUILD INDEX --------------------

def extract_index():
    conn = get_mysql_conn()
    print("MySQL Connected!")

    metadata = fetch_metadata(conn)
    print(f"Fetched {len(metadata)} columns")

    docs = []

    for m in metadata:

        # SAFE key fetch
        table = m.get("table_name") or m.get("TABLE_NAME")
        column = m.get("column_name") or m.get("COLUMN_NAME")
        schema = m.get("table_schema") or m.get("TABLE_SCHEMA")
        data_type = m.get("data_type") or m.get("DATA_TYPE")
        is_nullable = m.get("is_nullable") or m.get("IS_NULLABLE")
        comment = m.get("column_comment") or m.get("COLUMN_COMMENT")

        # profiling
        profile = profile_column(conn, table, column)

        # description
        blob = build_text_blob(
            {
                "table_schema": schema,
                "table_name": table,
                "column_name": column,
                "data_type": data_type,
                "is_nullable": is_nullable,
                "column_comment": comment
            },
            profile
        )

        # embeddings
        emb = get_embedding(blob)

        # FIXED timestamp format (no microseconds)
        timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        doc_id = f"{schema}_{table}_{column}".replace(".","_")

        doc = {
            "id": str(doc_id),
            "db": MYSQL_DB,
            "schema": schema,
            "table": table,
            "column": column,
            "data_type": data_type,
            "nullable": is_nullable == "YES",
            "comment": comment,
            "text_blob": blob,
            "embedding": emb,
            "sample_values": profile["sample_values"],
            "null_ratio": profile["null_ratio"],
            "approx_distinct": profile["approx_distinct"],
            "extracted_at": timestamp,
            "indexed_at": timestamp
        }

        docs.append(doc)
        print(f"Prepared {doc_id}")

    # Upload all together
    search_client.upload_documents(docs)
    print(f"Uploaded {len(docs)} docs")

    print("Index build completed.")
    conn.close()


# -------------------- SEARCH FUNCTION --------------------

def search_query(q: str):
    q_emb = get_embedding(q)

    vector_query = VectorizedQuery(
        vector=q_emb,
        k_nearest_neighbors=5,
        fields="embedding"
    )

    results = search_client.search(
        search_text=None,
        vector_queries=[vector_query]
    )

    print("\nSearch Results:\n")
    for i, r in enumerate(results, 1):
        print(f"{i}. {r['schema']}.{r['table']}.{r['column']}")
        print(f"   {r['text_blob'][:200]}...\n")


# -------------------- MAIN --------------------

if __name__ == "__main__":
    extract_index()

    while True:
        q = input("\nEnter search query (or 'exit'): ")
        if q.lower() == "exit":
            break
        search_query(q)