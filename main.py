import mysql.connector


# =========================
#   FUNCTION: GET METADATA
# =========================
def get_metadata():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Akash555",
            database="shop"
        )

        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'shop';
        """)

        tables = cursor.fetchall()

        all_metadata = []

        for tbl in tables:
            table_name = tbl["TABLE_NAME"]

            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_key,
                    column_comment
                FROM information_schema.columns
                WHERE table_schema = 'shop'
                AND table_name = '{table_name}';
            """)

            columns = cursor.fetchall()

            all_metadata.append({
                "table": table_name,
                "columns": columns
            })

        conn.close()
        return all_metadata

    except Exception as e:
        print("Error:", e)
        return []


# =========================
#   FUNCTION: PROFILE TABLE
# =========================
def profile_table(table_name):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Akash555",
            database="shop"
        )

        cursor = conn.cursor()

        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [col[0] for col in cursor.fetchall()]

        profile_output = {}

        for col in columns:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")
            null_count = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]

            cursor.execute(f"SELECT COUNT(DISTINCT {col}) FROM {table_name}")
            distinct_count = cursor.fetchone()[0]

            cursor.execute(f"SELECT MIN({col}) FROM {table_name}")
            min_val = cursor.fetchone()[0]

            cursor.execute(f"SELECT MAX({col}) FROM {table_name}")
            max_val = cursor.fetchone()[0]

            cursor.execute(f"SELECT {col} FROM {table_name} LIMIT 5")
            sample_vals = [r[0] for r in cursor.fetchall()]

            profile_output[col] = {
                "total_rows": total_rows,
                "null_count": null_count,
                "null_percentage": round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0,
                "distinct_count": distinct_count,
                "min": min_val,
                "max": max_val,
                "sample_value": sample_vals
            }

        conn.close()
        return profile_output

    except Exception as e:
        print("Profiling Error:", e)
        return {}


# =========================
#   FUNCTION: TEXT BLOB
# =========================
def generate_text_blob(table_name, metadata, profile):
    text_blobs = []

    for col in metadata:
        col_name = col["COLUMN_NAME"]
        data_type = col["DATA_TYPE"]
        nullable = col["IS_NULLABLE"]
        comment = col["COLUMN_COMMENT"]

        prof = profile[col_name]

        blob = f"""
Table: {table_name}
Column: {col_name}
Type: {data_type}
Nullable: {nullable}
Comment: {comment}

Profiling:
 - Total Rows: {prof['total_rows']}
 - Null Count: {prof['null_count']}
 - Null %: {prof['null_percentage']}
 - Distinct Count: {prof['distinct_count']}
 - Min: {prof['min']}
 - Max: {prof['max']}
 - Sample Values: {prof['sample_value']}

Summary:
This column '{col_name}' belongs to table '{table_name}'. 
It stores values of type '{data_type}'. 
Comment: {comment}.
Null percentage is {prof['null_percentage']}% with {prof['distinct_count']} distinct values.
Sample data values include {prof['sample_value']}.
"""

        text_blobs.append({
            "table": table_name,
            "column": col_name,
            "text_blob": blob.strip()
        })

    return text_blobs


# =========================
#     RUN TEXT BLOB GENERATION
# =========================
print("\n===== GENERATING TEXT BLOBS =====")

metadata_list = get_metadata()

for t in metadata_list:
    table = t["table"]
    cols = t["columns"]

    prof = profile_table(table)
    blobs = generate_text_blob(table, cols, prof)

    for b in blobs:
        print("\n----------------------------")
        print(b["text_blob"])
        print("----------------------------")
