import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASS = os.getenv("PG_PASSWORD")
DB_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT")

CSV_PATH = os.path.join("data", "transactions_data.csv")
DATE_P1 = "2025-01-01"
CHUNKSIZE = 5000

def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def recreate_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS transactions CASCADE;")
        cur.execute("""
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY,
                date TIMESTAMP,
                client_id TEXT,
                card_id TEXT,
                amount REAL,
                use_chip TEXT,
                merchant_id TEXT,
                merchant_city TEXT,
                merchant_state TEXT,
                zip TEXT,
                mcc TEXT,
                errors TEXT,
                date_insertion DATE
            );
        """)
    conn.commit()

def insert_data(conn):
    total_rows = sum(1 for _ in open(CSV_PATH)) - 1
    max_rows = int(total_rows * 0.7)
    inserted_rows = 0
    chunk_idx = 0

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNKSIZE):
        if inserted_rows >= max_rows:
            break
        chunk_idx += 1

        chunk = chunk[pd.to_numeric(chunk['id'], errors='coerce').notnull()]
        chunk['id'] = chunk['id'].astype(int)
        chunk['amount'] = chunk['amount'].replace('[\$,]', '', regex=True).astype(float)
        chunk['date_insertion'] = DATE_P1

        if inserted_rows + len(chunk) > max_rows:
            chunk = chunk.iloc[:max_rows - inserted_rows]

        values = [
            (
                row['id'], row['date'], row['client_id'], row['card_id'], row['amount'], row['use_chip'],
                row['merchant_id'], row['merchant_city'], row['merchant_state'], str(row['zip']), row['mcc'], row['errors'],
                row['date_insertion']
            )
            for _, row in chunk.iterrows()
        ]

        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO transactions (id, date, client_id, card_id, amount, use_chip,
                    merchant_id, merchant_city, merchant_state, zip, mcc, errors, date_insertion)
                VALUES %s
                ON CONFLICT (id) DO NOTHING;
            """, values)
        conn.commit()

        inserted_rows += len(chunk)
        print(f"✅ Chunk {chunk_idx} traité ({len(chunk)} lignes).")

    print(f"📥 Insertion terminée : {inserted_rows} lignes insérées (70% du total) avec date {DATE_P1}.")

def main():
    conn = None
    try:
        conn = connect()
        recreate_table(conn)
        insert_data(conn)
    except Exception as e:
        print(f"❌ Erreur : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()














# import sqlite3
# import os
# import pandas as pd

# DB_DIR = "database"
# DB_PATH = os.path.join(DB_DIR, "database.db")
# CSV_PATH = os.path.join("data", "transactions_data.csv")
# DATE_P1 = "2025-01-01"

# if not os.path.exists(DB_DIR):
#     os.makedirs(DB_DIR)

# try:
#     conn = sqlite3.connect(DB_PATH)
#     cursor = conn.cursor()

#     # Lecture colonnes CSV
#     df = pd.read_csv(CSV_PATH)
#     csv_columns = df.columns.tolist()
#     print(f"Colonnes CSV : {csv_columns}")

#     # Création table transactions
#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions';")
#     if not cursor.fetchone():
#         cursor.execute(f"""
#             CREATE TABLE transactions (
#                 id INTEGER PRIMARY KEY,
#                 date TEXT,
#                 client_id TEXT,
#                 card_id TEXT,
#                 amount REAL,
#                 use_chip INTEGER,
#                 merchant_id TEXT,
#                 merchant_city TEXT,
#                 merchant_state TEXT,
#                 zip TEXT,
#                 mcc TEXT,
#                 errors TEXT
#             );
#         """)
#         print("Table 'transactions' créée.")
    
#     # Création table situation_date
#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='situation_date';")
#     if not cursor.fetchone():
#         cursor.execute("""
#             CREATE TABLE situation_date (
#                 transaction_id INTEGER,
#                 date_insertion TEXT,
#                 FOREIGN KEY(transaction_id) REFERENCES transactions(id)
#             );
#         """)
#         print("Table 'situation_date' créée.")

#     # Vérifie si transactions vide
#     cursor.execute("SELECT COUNT(*) FROM transactions;")
#     if cursor.fetchone()[0] == 0:
#         df_p1 = df.iloc[:int(len(df) * 0.7)]
#         df_p1.to_sql("transactions", conn, if_exists='append', index=False)
#         print(f" Données P1 (70%) insérées dans 'transactions'.")

#         for trans_id in df_p1["id"]:
#             cursor.execute("""
#                 INSERT INTO situation_date (transaction_id, date_insertion) VALUES (?, ?)
#             """, (trans_id, DATE_P1))
#         conn.commit()
#         print(f" Date {DATE_P1} ajoutée dans 'situation_date' pour P1.")
#     else:
#         print(" Table transactions déjà remplie. Aucune insertion.")

# except Exception as e:
#     print(f" Erreur : {e}")

# finally:
#     if conn:
#         conn.close()
