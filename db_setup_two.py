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
DATE_P2 = "2025-01-02"
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
    chunk_idx = 0
    total_inserted = 0

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNKSIZE):
        chunk_idx += 1

        chunk = chunk[pd.to_numeric(chunk['id'], errors='coerce').notnull()]
        chunk['id'] = chunk['id'].astype(int)
        chunk['amount'] = chunk['amount'].replace('[\$,]', '', regex=True).astype(float)
        chunk['date_insertion'] = DATE_P2

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

        total_inserted += len(chunk)
        print(f" Chunk {chunk_idx} traité ({len(chunk)} lignes).")

    print(f" Insertion terminée : {total_inserted} lignes insérées (100% du total) avec date {DATE_P2}.")

def main():
    conn = None
    try:
        conn = connect()
        recreate_table(conn)
        insert_data(conn)
    except Exception as e:
        print(f" Erreur : {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()





















# import sqlite3
# import os
# import pandas as pd

# # Chemins
# DB_DIR = "database"
# DB_PATH = os.path.join(DB_DIR, "database.db")
# CSV_PATH = os.path.join("data", "transactions_data.csv")
# DATE_INSERTION = "2025-01-02"

# CHUNKSIZE = 10000  # nombre de lignes à traiter par batch

# # Création dossier database si nécessaire
# if not os.path.exists(DB_DIR):
#     os.makedirs(DB_DIR)

# try:
#     # Connexion DB
#     conn = sqlite3.connect(DB_PATH)
#     cursor = conn.cursor()

#     # Suppression des anciennes tables
#     print(" Suppression des anciennes tables...")
#     cursor.execute("DROP TABLE IF EXISTS situation_date;")
#     cursor.execute("DROP TABLE IF EXISTS transactions;")
#     conn.commit()

#     # Création des nouvelles tables
#     print(" Création des tables...")
#     cursor.execute("""
#         CREATE TABLE transactions (
#             id INTEGER PRIMARY KEY,
#             date TEXT,
#             client_id TEXT,
#             card_id TEXT,
#             amount REAL,
#             use_chip INTEGER,
#             merchant_id TEXT,
#             merchant_city TEXT,
#             merchant_state TEXT,
#             zip TEXT,
#             mcc TEXT,
#             errors TEXT
#         );
#     """)
#     cursor.execute("""
#         CREATE TABLE situation_date (
#             transaction_id INTEGER,
#             date_insertion TEXT,
#             FOREIGN KEY(transaction_id) REFERENCES transactions(id)
#         );
#     """)
#     conn.commit()

#     # Chargement par chunk
#     print(" Chargement et insertion par batch...")
#     inserted_rows = 0
#     for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNKSIZE)):
#         chunk.to_sql("transactions", conn, if_exists='append', index=False)

#         # Historisation dans situation_date
#         situation_data = [(int(tid), DATE_INSERTION) for tid in chunk["id"]]
#         cursor.executemany("""
#             INSERT INTO situation_date (transaction_id, date_insertion) VALUES (?, ?);
#         """, situation_data)
#         conn.commit()

#         inserted_rows += len(chunk)
#         print(f" Batch {i+1}: {len(chunk)} lignes insérées (total: {inserted_rows})")

#     print(f" Chargement complet : {inserted_rows} lignes insérées dans transactions et situation_date.")

# except Exception as e:
#     print(f" Erreur : {e}")

# finally:
#     if conn:
#         conn.close()