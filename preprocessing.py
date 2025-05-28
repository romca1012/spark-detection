from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
import os

# SparkSession
spark = SparkSession.builder \
    .appName("Bronze to Silver - Preprocessing") \
    .config("spark.sql.warehouse.dir", "hdfs://hadoop-master:9000/root/tp_fraude/spark-warehouse") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
    .config("spark.sql.files.ignoreMissingFiles", "true") \
    .enableHiveSupport() \
    .getOrCreate()


# .config("spark.sql.files.ignoreCorruptFiles", "true") \
# .config("spark.sql.files.ignoreMissingFiles", "true") \

# Fonction pour récupérer le chemin le plus récent
def get_latest_path(base_path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
    dirs = fs.listStatus(path)
    all_dirs = [str(d.getPath().getName()) for d in dirs if d.isDirectory()]
    all_dirs = [d for d in all_dirs if d[:4].isdigit()]
    latest_date = sorted(all_dirs)[-1]
    return os.path.join(base_path, latest_date)

# Lecture des datasets depuis Bronze
bronze_base = "hdfs://hadoop-master:9000/tp_fraude/bronze"
df_transactions = spark.read.parquet(get_latest_path(os.path.join(bronze_base, "csv_transactions")))
df_users = spark.read.parquet(get_latest_path(os.path.join(bronze_base, "users_data")))
df_cards = spark.read.parquet(get_latest_path(os.path.join(bronze_base, "cards_data")))
df_mcc = spark.read.parquet(get_latest_path(os.path.join(bronze_base, "mcc_codes")))
df_labels = spark.read.parquet(get_latest_path(os.path.join(bronze_base, "train_fraud_labels")))

# Renommage des colonnes pour éviter les conflits
df_users = df_users.withColumnRenamed("id", "client_id")
df_cards = df_cards.withColumnRenamed("id", "card_id") \
                   .withColumnRenamed("client_id", "card_client_id")
df_mcc = df_mcc.withColumnRenamed("mcc_code", "mcc")
df_labels = df_labels.withColumnRenamed("transaction_id", "label_transaction_id")

# Jointures explicites
df_joined = df_transactions \
    .join(df_users, on="client_id", how="left") \
    .join(df_cards, on="card_id", how="left") \
    .join(df_mcc, on="mcc", how="left") \
    .join(df_labels, df_transactions["id"] == col("label_transaction_id"), how="left") \
    .drop("label_transaction_id")

# Nettoyage des colonnes : suppression des montants négatifs
df_joined = df_joined.filter(col("amount") > 0)

# Nettoyage des champs monétaires
df_joined = df_joined.withColumn("credit_limit", regexp_replace("credit_limit", "[\\$,]", "").cast("double"))
df_joined = df_joined.withColumn("per_capita_income", regexp_replace("per_capita_income", "[\\$,]", "").cast("double"))
df_joined = df_joined.withColumn("yearly_income", regexp_replace("yearly_income", "[\\$,]", "").cast("double"))
df_joined = df_joined.withColumn("total_debt", regexp_replace("total_debt", "[\\$,]", "").cast("double"))

# Encodage du label
if "label" in df_joined.columns:
    df_joined = df_joined.withColumn("label", when(col("label") == "Yes", 1).otherwise(0))

# Suppression des lignes critiques si certaines clés sont nulles (ex: id)
df_joined = df_joined.filter(col("id").isNotNull())

# Créer la base Hive si elle n'existe pas
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Sauvegarde dans Hive

df_joined.write.mode("overwrite").saveAsTable("silver.transactions_cleaned")

# Fermeture propre
spark.stop()
