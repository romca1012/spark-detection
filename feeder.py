import os
import json
import shutil
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Chargement des variables d'environnement
load_dotenv()

PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")

# Dossiers
BRONZE_DIR = Path("bronze")
DATA_DIR = Path("data")
TRANSACTIONS_PARQUET_DIR = BRONZE_DIR / "csv_transactions"
STATIC_DATES = ["2025-01-01", "2025-01-02"]

# Initialisation Spark
spark = SparkSession.builder \
    .appName("Feeder") \
    .config("spark.jars", "/home/someone_specially/spark-fraud-detection/jars/postgresql-42.7.5.jar") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()

BRONZE_DIR.mkdir(exist_ok=True)
TRANSACTIONS_PARQUET_DIR.mkdir(exist_ok=True)

def historise_transactions():
    try:
        query_dates = "(SELECT DISTINCT date_insertion FROM transactions) AS dates_sub"
        df_dates = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
            .option("dbtable", query_dates) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        print("=== DATES DÉTECTÉES DANS LA BASE ===")
        df_dates.show()

        for row in df_dates.collect():
            date_str = row["date_insertion"].strftime("%Y-%m-%d")
            date_path = TRANSACTIONS_PARQUET_DIR / date_str
            if date_path.exists():
                print(f"⏩ Déjà historisé : {date_path}")
                continue

            print(f"\n📅 Traitement de la date : {date_str}")
            print(f"📂 Dossier de sortie prévu : {date_path}")

            bounds_query = f"""
                (SELECT MIN(id) AS lo, MAX(id) AS hi
                 FROM transactions
                 WHERE date_insertion = '{date_str}') AS bounds_sub
            """
            bounds = spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
                .option("dbtable", bounds_query) \
                .option("user", PG_USER) \
                .option("password", PG_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .collect()[0]

            lower, upper = bounds["lo"], bounds["hi"]
            print(f"🔢 ID min = {lower}, ID max = {upper}")

            if lower is None or upper is None:
                print(f"⚠️ Aucune donnée pour la date {date_str}")
                continue

            df_data = spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
                .option("dbtable", f"(SELECT * FROM transactions WHERE date_insertion = '{date_str}') AS t") \
                .option("partitionColumn", "id") \
                .option("lowerBound", lower) \
                .option("upperBound", upper) \
                .option("numPartitions", 4) \
                .option("fetchsize", 10000) \
                .option("user", PG_USER) \
                .option("password", PG_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            print(f"🔍 Nombre de lignes récupérées : {df_data.count()}")
            print(f"📑 Colonnes : {df_data.columns}")

            df_data.write.mode("overwrite").parquet(str(date_path))
            print(f"✅ Transactions historisées : {date_path}")

    except Exception as e:
        print(f"❌ Erreur pendant l'historisation des transactions : {e}")

def historise_static_files():
    import ijson

    for file in DATA_DIR.iterdir():
        if file.name == "transactions_data.csv":
            continue

        first_date_dir = BRONZE_DIR / file.stem / STATIC_DATES[0]
        if first_date_dir.exists():
            print(f"⏩ Déjà historisé : {first_date_dir}")
            continue

        try:
            print(f"\n📄 Lecture de : {file.name}")

            if file.suffix == ".csv":
                df = spark.read.option("header", True).option("inferSchema", True).csv(str(file))

            elif file.name == "mcc_codes.json":
                with open(file, "r", encoding="utf-8") as f:
                    parsed = json.load(f)
                rows = [{"mcc_code": k, "description": v} for k, v in parsed.items()]
                df = spark.createDataFrame(rows)

            elif file.name == "train_fraud_labels.json":
                print("🔄 Lecture optimisée de train_fraud_labels.json ...")
                with open(file, "r", encoding="utf-8") as f:
                    parser = ijson.kvitems(f, "target")
                    rdd = spark.sparkContext.parallelize(
                        ({"transaction_id": k, "label": v} for k, v in parser),
                        numSlices=4
                    )
                    df = spark.createDataFrame(rdd)

            elif file.suffix == ".json":
                df = spark.read \
                    .option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .json(str(file)) \
                    .filter(F.col("_corrupt_record").isNull()) \
                    .drop("_corrupt_record")

            else:
                print(f"❗ Format non supporté : {file.name}")
                continue

            if not df.columns:
                print(f"⚠️ Schéma vide détecté dans {file.name} → ignoré")
                continue

            df = df.repartition(4)
            first_date_dir.mkdir(parents=True, exist_ok=True)
            df.write.mode("overwrite").parquet(str(first_date_dir))
            print(f"✅ {file.name} historisé une fois → {first_date_dir}")

            for date_str in STATIC_DATES[1:]:
                target_dir = BRONZE_DIR / file.stem / date_str
                if target_dir.exists():
                    shutil.rmtree(target_dir)
                shutil.copytree(first_date_dir, target_dir)
                print(f"📁 Copie vers {target_dir}")

        except Exception as e:
            print(f"❌ Erreur lors du traitement de {file.name} : {e}")

if __name__ == "__main__":
    historise_transactions()
    historise_static_files()
    spark.stop()