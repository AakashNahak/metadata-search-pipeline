# Metadata Search Pipeline (MySQL + Azure OpenAI + Azure Cognitive Search)

## 1. Project Overview

This project implements an end-to-end **semantic metadata search pipeline** for a MySQL database.  
Instead of manually browsing tables and columns, a user can type a natural-language query (for example, _"email"_ or _"columns related to order dates"_), and the system returns the most relevant database columns and tables.

The pipeline combines:

- **MySQL** – source database and schema metadata  
- **Python** – ETL, profiling, orchestration and CLI  
- **Azure OpenAI** – text embeddings (`text-embedding-3-large`)  
- **Azure Cognitive Search** – vector index and semantic search

This repository is suitable for a **Data Engineer assignment / interview project** and demonstrates how to design and implement a modern metadata search system.

---

## 2. High-Level Architecture

1. **MySQL (shop database)**  
   - Tables: `customers`, `orders` (sample schema)  
   - Acts as the source system for metadata and profiling.

2. **Python Metadata Extractor & Profiler**  
   - Connects to MySQL using `mysql-connector-python`.  
   - Reads `information_schema.columns` to extract:
     - database / schema / table / column names  
     - data type, nullable flag, column comments  
   - Performs light profiling per column:
     - `null_ratio`  
     - `approx_distinct` (COUNT DISTINCT)  
     - `sample_values` (up to 5 examples)

3. **Text Blob Generator (Python)**  
   Builds a **human-readable description** for each column combining metadata + profiling, e.g.:

   > “Column `email` in table `customers` of database `shop`. Datatype: varchar. It is not nullable. Comment: customer email address. Null ratio: 0.00. Approx distinct: 100. Samples: john@example.com, …”

4. **Azure OpenAI – Embedding Service**  
   - Deployed model: `text-embedding-3-large`  
   - Embedding size: **3072 dimensions**  
   - Each text blob is converted into a vector representation using Azure OpenAI SDK.

5. **Azure Cognitive Search – Vector Index**  
   - Index name: `metadata-search-index`  
   - Contains:
     - `id` (composed key: `schema_table_column`)  
     - `db`, `schema`, `table`, `column`  
     - `data_type`, `nullable`, `comment`  
     - profiling fields (`null_ratio`, `approx_distinct`, `sample_values`)  
     - `text_blob` (original description)  
     - `embedding` (vector field)  
     - `extracted_at`, `indexed_at`  

6. **Semantic Search CLI (Python)**  
   - User types a natural language query at the command line.  
   - The query is embedded via Azure OpenAI.  
   - Azure Cognitive Search performs vector KNN search over `embedding`.  
   - Top-k matching columns are printed with their descriptions.

---

## 3. Tech Stack

- **Language**: Python 3.10+  
- **Database**: MySQL (local or RDS)  
- **Cloud AI**: Azure OpenAI (`text-embedding-3-large`)  
- **Search Engine**: Azure Cognitive Search (vector search)  
- **Libraries**:
  - `mysql-connector-python`
  - `openai` (Azure OpenAI SDK)
  - `azure-search-documents`
  - `python-dotenv`

---

## 4. Repository Structure

```text
.
├── Azure_con.py                # Main pipeline script (ETL + embeddings + indexing + CLI)
├── test_embedding.py           # Simple script to test Azure OpenAI connection
├── requirements.txt            # Python dependencies
├── .env.example                # Template for environment variables (without secrets)
├── README.md                   # Project documentation
└── docs/
    ├── architecture.png        # Architecture diagram (optional)
    └── Metadata_Documentation.docx   # Detailed documentation (optional)
