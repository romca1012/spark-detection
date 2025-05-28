from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Initialiser Spark avec support Hive
spark = SparkSession.builder \
    .appName("Export Silver to PostgreSQL") \
    .config("spark.sql.warehouse.dir", "hdfs://hadoop-master:9000/root/tp_fraude/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Lire depuis Hive (Silver)
spark.sql("USE silver")
df_silver = spark.sql("SELECT * FROM transactions_cleaned WHERE label IS NOT NULL")

# Charger le modèle GBT depuis HDFS
model = PipelineModel.load("hdfs://hadoop-master:9000/root/tp_fraude/models/best_model")

# Appliquer le modèle
df_pred = model.transform(df_silver)

# Définir une UDF pour extraire la probabilité
def extract_prob(v):
    try:
        return float(v[0])  # GBT renvoie la proba pour classe positive en première valeur
    except:
        return 0.0

extract_prob_udf = udf(extract_prob, DoubleType())
df_pred = df_pred.withColumn("fraud_probability", extract_prob_udf(col("probability")))

# 🔻 Sélectionner uniquement les colonnes utiles pour Power BI (évite OOM)
df_gold = df_pred.select(
    "id", "client_id", "amount",
    "merchant_city", "merchant_state",
    "label",
    col("prediction").alias("label_pred"),
    col("fraud_probability")
)

# (Optionnel) Sauvegarde en Parquet intermédiaire (utile pour debug ou import alternatif)
df_gold.repartition(4).write.mode("overwrite").parquet("/root/export/gold_transactions_scored")

# Écriture finale dans PostgreSQL
df_gold.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}") \
    .option("dbtable", "gold.transactions_scored") \
    .option("user", PG_USER) \
    .option("password", PG_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", "1000") \
    .option("numPartitions", "4") \
    .mode("overwrite") \
    .save()

print("✅ Table gold.transactions_scored exportée avec succès.")
spark.stop()
