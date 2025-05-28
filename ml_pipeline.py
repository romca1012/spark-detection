from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
import os

# Spark session
spark = SparkSession.builder \
    .appName("Optimized GBT Pipeline") \
    .config("spark.sql.warehouse.dir", "hdfs://hadoop-master:9000/root/tp_fraude/spark-warehouse") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.executor.instances", "1") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE silver")
df = spark.sql("SELECT * FROM transactions_cleaned WHERE label IS NOT NULL")

# Drop nulls in key features
key_features = ["amount", "credit_score", "num_credit_cards", "num_cards_issued", "latitude", "longitude", "label"]
df = df.dropna(subset=key_features)

# Balance the dataset
count_0 = df.filter(col("label") == 0).count()
count_1 = df.filter(col("label") == 1).count()
minority = min(count_0, count_1)
df_0 = df.filter(col("label") == 0).sample(False, float(minority) / count_0, seed=42)
df_1 = df.filter(col("label") == 1).sample(False, float(minority) / count_1, seed=42)
df = df_0.union(df_1)

# StringIndexing
indexers = [
    StringIndexer(inputCol="use_chip", outputCol="use_chip_idx", handleInvalid="keep"),
    StringIndexer(inputCol="card_brand", outputCol="card_brand_idx", handleInvalid="keep"),
    StringIndexer(inputCol="card_type", outputCol="card_type_idx", handleInvalid="keep"),
    StringIndexer(inputCol="has_chip", outputCol="has_chip_idx", handleInvalid="keep"),
    StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep"),
    StringIndexer(inputCol="label", outputCol="label_idx")
]

# Features
features = [
    "amount", "credit_score", "num_credit_cards", "num_cards_issued", "latitude", "longitude",
    "use_chip_idx", "card_brand_idx", "card_type_idx", "has_chip_idx", "gender_idx"
]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# GBT Classifier (allégé)
clf = GBTClassifier(labelCol="label_idx", featuresCol="features", maxIter=20, maxDepth=5, seed=42)

# Pipeline
pipeline = Pipeline(stages=indexers + [assembler, clf])
train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)
preds = model.transform(test)

# Evaluation
pred_and_labels = preds.select("prediction", "label_idx").rdd.map(lambda r: (float(r[0]), float(r[1])))
metrics = MulticlassMetrics(pred_and_labels)

precision = metrics.precision(1.0)
recall = metrics.recall(1.0)
f1 = metrics.fMeasure(1.0)
acc = metrics.accuracy

summary = f"""
===== GBTClassifier (Optimized) =====
Accuracy: {acc:.4f}
Precision: {precision:.4f}
Recall: {recall:.4f}
F1-score: {f1:.4f}
"""

# Affichage et sauvegarde immédiate avant tout autre appel
print(summary)
os.makedirs("scripts", exist_ok=True)
with open("scripts/metrics_optimized.txt", "w") as f:
    f.write(summary)

# Enregistrement du modèle (à la fin)
model.write().overwrite().save("hdfs://hadoop-master:9000/root/tp_fraude/models/best_model")

spark.stop()






















# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.ml.feature import StringIndexer, VectorAssembler
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.ml import Pipeline
# from pyspark.mllib.evaluation import MulticlassMetrics
# from pyspark.storagelevel import StorageLevel

# # Spark session
# spark = SparkSession.builder \
#     .appName("ML Pipeline - Balanced Fraud Detection") \
#     .config("spark.sql.warehouse.dir", "hdfs://hadoop-master:9000/root/tp_fraude/spark-warehouse") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.executor.instances", "1") \
#     .config("spark.executor.cores", "1") \
#     .config("spark.sql.shuffle.partitions", "8") \
#     .enableHiveSupport() \
#     .getOrCreate()

# # Load data
# spark.sql("USE silver")
# df = spark.sql("SELECT * FROM transactions_cleaned WHERE label IS NOT NULL")

# # Drop nulls
# important_features = [
#     "amount", "credit_score", "num_credit_cards", "num_cards_issued",
#     "latitude", "longitude", "label"
# ]
# df = df.dropna(subset=important_features)

# # Count for balancing
# count_class_0 = df.filter(col("label") == 0).count()
# count_class_1 = df.filter(col("label") == 1).count()
# minority_class_size = min(count_class_0, count_class_1)

# df_0 = df.filter(col("label") == 0).sample(False, float(minority_class_size) / count_class_0, seed=42)
# df_1 = df.filter(col("label") == 1).sample(False, float(minority_class_size) / count_class_1, seed=42)
# df = df_0.union(df_1)
# print(f"Balanced dataset size: {df.count()}")

# # Class weight
# fraud_ratio = df.filter(col("label") == 0).count() / df.filter(col("label") == 1).count()
# df = df.withColumn("classWeightCol", col("label").cast("double") * fraud_ratio + (1 - col("label").cast("double")))

# # Indexing
# indexers = [
#     StringIndexer(inputCol="use_chip", outputCol="use_chip_index", handleInvalid="keep"),
#     StringIndexer(inputCol="card_brand", outputCol="card_brand_index", handleInvalid="keep"),
#     StringIndexer(inputCol="card_type", outputCol="card_type_index", handleInvalid="keep"),
#     StringIndexer(inputCol="has_chip", outputCol="has_chip_index", handleInvalid="keep"),
#     StringIndexer(inputCol="gender", outputCol="gender_index", handleInvalid="keep"),
#     StringIndexer(inputCol="label", outputCol="label_index")
# ]

# # Feature engineering
# feature_cols = [
#     "amount", "credit_score", "num_credit_cards", "num_cards_issued",
#     "latitude", "longitude",
#     "use_chip_index", "card_brand_index", "card_type_index",
#     "has_chip_index", "gender_index"
# ]
# assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# # RandomForest
# classifier = RandomForestClassifier(
#     labelCol="label_index",
#     featuresCol="features",
#     numTrees=100,
#     maxDepth=8,
#     weightCol="classWeightCol",
#     seed=42
# )

# # Pipeline
# pipeline = Pipeline(stages=indexers + [assembler, classifier])

# # Split
# train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
# train_data.persist(StorageLevel.MEMORY_AND_DISK)
# test_data.persist(StorageLevel.MEMORY_AND_DISK)

# # Train
# model = pipeline.fit(train_data)

# # Predict
# predictions = model.transform(test_data).persist(StorageLevel.MEMORY_AND_DISK)

# # Eval
# evaluator_auc = BinaryClassificationEvaluator(labelCol="label_index", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
# evaluator_acc = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")

# auc = evaluator_auc.evaluate(predictions)
# accuracy = evaluator_acc.evaluate(predictions)

# # Multiclass metrics
# predictionAndLabels = predictions.select("prediction", "label_index") \
#     .rdd.map(lambda r: (float(r.prediction), float(r.label_index)))
# metrics = MulticlassMetrics(predictionAndLabels)
# conf_matrix = metrics.confusionMatrix().toArray()
# precision = metrics.precision(1.0)
# recall = metrics.recall(1.0)
# f1 = metrics.fMeasure(1.0)

# # Print
# print("=" * 80)
# print(f"Accuracy: {accuracy:.4f}")
# print(f"AUC: {auc:.4f}")
# print(f"Fraud class (1) - Precision: {precision:.4f}")
# print(f"Fraud class (1) - Recall:    {recall:.4f}")
# print(f"Fraud class (1) - F1-score:  {f1:.4f}")
# print("Confusion matrix:")
# print(conf_matrix)
# print("=" * 80)

# # Save
# with open("metrics_balanced.txt", "w") as f:
#     f.write(f"Accuracy: {accuracy:.4f}\n")
#     f.write(f"AUC: {auc:.4f}\n")
#     f.write(f"Precision (fraud): {precision:.4f}\n")
#     f.write(f"Recall (fraud):    {recall:.4f}\n")
#     f.write(f"F1-score (fraud):  {f1:.4f}\n")
#     f.write("Confusion matrix:\n")
#     f.write(str(conf_matrix))

# model.write().overwrite().save("hdfs://hadoop-master:9000/root/tp_fraude/models/rf_model_balanced")

# spark.stop()



































# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.ml.feature import StringIndexer, VectorAssembler
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.ml import Pipeline
# from pyspark.mllib.evaluation import MulticlassMetrics
# from pyspark.storagelevel import StorageLevel

# # Spark session
# spark = SparkSession.builder \
#     .appName("ML Pipeline - Medium Fraud Detection") \
#     .config("spark.sql.warehouse.dir", "hdfs://hadoop-master:9000/root/tp_fraude/spark-warehouse") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.executor.instances", "1") \
#     .config("spark.executor.cores", "1") \
#     .config("spark.sql.shuffle.partitions", "8") \
#     .enableHiveSupport() \
#     .getOrCreate()


# # Load data from Hive
# spark.sql("USE silver")
# df = spark.sql("SELECT * FROM transactions_cleaned WHERE label IS NOT NULL")

# # Drop rows with nulls in important features
# important_features = [
#     "amount", "credit_score", "num_credit_cards", "num_cards_issued",
#     "latitude", "longitude", "label"
# ]
# df = df.dropna(subset=important_features)

# # Sample for performance only if needed
# row_count = df.count()
# print(f"Total rows before sampling: {row_count}")
# df = df.sample(False, 0.1, seed=42)
# print(f"Rows after sampling: {df.count()}")

# # Handle class imbalance
# fraud_ratio = df.filter(col("label") == 0).count() / df.filter(col("label") == 1).count()
# df = df.withColumn("classWeightCol", col("label").cast("double") * fraud_ratio + (1 - col("label").cast("double")))

# # String indexing
# indexers = [
#     StringIndexer(inputCol="use_chip", outputCol="use_chip_index", handleInvalid="keep"),
#     StringIndexer(inputCol="card_brand", outputCol="card_brand_index", handleInvalid="keep"),
#     StringIndexer(inputCol="card_type", outputCol="card_type_index", handleInvalid="keep"),
#     StringIndexer(inputCol="has_chip", outputCol="has_chip_index", handleInvalid="keep"),
#     StringIndexer(inputCol="gender", outputCol="gender_index", handleInvalid="keep"),
#     StringIndexer(inputCol="label", outputCol="label_index")
# ]

# # Feature assembly
# feature_cols = [
#     "amount", "credit_score", "num_credit_cards", "num_cards_issued",
#     "latitude", "longitude",
#     "use_chip_index", "card_brand_index", "card_type_index",
#     "has_chip_index", "gender_index"
# ]
# assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# # Random Forest classifier
# classifier = RandomForestClassifier(
#     labelCol="label_index",
#     featuresCol="features",
#     numTrees=50,
#     maxDepth=6,
#     weightCol="classWeightCol",
#     seed=42
# )

# # Full pipeline
# pipeline = Pipeline(stages=indexers + [assembler, classifier])

# # Train/test split
# train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
# train_data.persist(StorageLevel.MEMORY_AND_DISK)
# test_data.persist(StorageLevel.MEMORY_AND_DISK)

# # Train model
# model = pipeline.fit(train_data)

# # Make predictions
# predictions = model.transform(test_data).persist(StorageLevel.MEMORY_AND_DISK)

# # Evaluate
# evaluator_auc = BinaryClassificationEvaluator(labelCol="label_index", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
# evaluator_acc = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")

# auc = evaluator_auc.evaluate(predictions)
# accuracy = evaluator_acc.evaluate(predictions)

# # Multiclass metrics
# predictionAndLabels = predictions.select("prediction", "label_index") \
#     .rdd.map(lambda r: (float(r.prediction), float(r.label_index)))
# metrics = MulticlassMetrics(predictionAndLabels)
# conf_matrix = metrics.confusionMatrix().toArray()
# precision = metrics.precision(1.0)
# recall = metrics.recall(1.0)
# f1 = metrics.fMeasure(1.0)

# # Print metrics
# print("=" * 80)
# print(f"Accuracy: {accuracy:.4f}")
# print(f"AUC: {auc:.4f}")
# print(f"Fraud class (1) - Precision: {precision:.4f}")
# print(f"Fraud class (1) - Recall:    {recall:.4f}")
# print(f"Fraud class (1) - F1-score:  {f1:.4f}")
# print("Confusion matrix:")
# print(conf_matrix)
# print("=" * 80)

# # Save metrics
# with open("metrics_medium.txt", "w") as f:
#     f.write(f"Accuracy: {accuracy:.4f}\n")
#     f.write(f"AUC: {auc:.4f}\n")
#     f.write(f"Precision (fraud): {precision:.4f}\n")
#     f.write(f"Recall (fraud):    {recall:.4f}\n")
#     f.write(f"F1-score (fraud):  {f1:.4f}\n")
#     f.write("Confusion matrix:\n")
#     f.write(str(conf_matrix))

# # Save model
# model.write().overwrite().save("hdfs://hadoop-master:9000/root/tp_fraude/models/rf_model_medium")


# # Clean
# spark.stop()













# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from pyspark.ml.feature import StringIndexer, VectorAssembler
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.ml import Pipeline
# from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
# import matplotlib.pyplot as plt

# # Spark session
# spark = SparkSession.builder \
#     .appName("ML Pipeline - Fraud Detection") \
#     .config("spark.sql.warehouse.dir", "hdfs://hadoop-master:9000/root/tp_fraude/spark-warehouse") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.driver.memory", "1g") \
#     .config("spark.executor.instances", "1") \
#     .config("spark.executor.cores", "1") \
#     .enableHiveSupport() \
#     .getOrCreate()

# # Load data from Hive
# spark.sql("USE silver")
# df = spark.sql("SELECT * FROM transactions_cleaned WHERE label IS NOT NULL")

# # Drop nulls
# important_features = [
#     "amount", "credit_score", "num_credit_cards", "num_cards_issued",
#     "latitude", "longitude", "label"
# ]
# df = df.dropna(subset=important_features)

# # Sampling for performance
# row_count = df.count()
# print(f"Total rows: {row_count}")
# if row_count > 50000:
#     df = df.sample(False, 0.3, seed=42)
#     print(f"Sampled dataset to reduce memory usage.")

# # Gérer le déséquilibre avec une pondération
# fraud_ratio = df.filter(col("label") == 0).count() / df.filter(col("label") == 1).count()
# df = df.withColumn("classWeightCol", col("label").cast("double") * fraud_ratio + (1 - col("label").cast("double")))

# # Indexing
# indexers = [
#     StringIndexer(inputCol="use_chip", outputCol="use_chip_index", handleInvalid="keep"),
#     StringIndexer(inputCol="card_brand", outputCol="card_brand_index", handleInvalid="keep"),
#     StringIndexer(inputCol="card_type", outputCol="card_type_index", handleInvalid="keep"),
#     StringIndexer(inputCol="has_chip", outputCol="has_chip_index", handleInvalid="keep"),
#     StringIndexer(inputCol="gender", outputCol="gender_index", handleInvalid="keep"),
#     StringIndexer(inputCol="label", outputCol="label_index")
# ]

# # Feature assembler
# feature_cols = [
#     "amount", "credit_score", "num_credit_cards", "num_cards_issued",
#     "latitude", "longitude",
#     "use_chip_index", "card_brand_index", "card_type_index",
#     "has_chip_index", "gender_index"
# ]
# assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# # Classifier
# classifier = RandomForestClassifier(
#     labelCol="label_index",
#     featuresCol="features",
#     numTrees=50,
#     weightCol="classWeightCol"
# )

# # Pipeline
# pipeline = Pipeline(stages=indexers + [assembler, classifier])

# # Split
# train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# # Train
# model = pipeline.fit(train_data)

# # Predict
# predictions = model.transform(test_data).cache()

# # Evaluation
# evaluator_auc = BinaryClassificationEvaluator(labelCol="label_index", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
# evaluator_acc = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")

# auc = evaluator_auc.evaluate(predictions)
# accuracy = evaluator_acc.evaluate(predictions)

# # Prepare for metrics
# predictionAndLabels = predictions.select("prediction", "label_index") \
#     .rdd.map(lambda r: (float(r.prediction), float(r.label_index)))

# metrics = MulticlassMetrics(predictionAndLabels)
# conf_matrix = metrics.confusionMatrix().toArray()

# precision = metrics.precision(1.0)
# recall = metrics.recall(1.0)
# f1 = metrics.fMeasure(1.0)

# # Print
# print("=" * 80)
# print(f"Accuracy: {accuracy:.4f}")
# print(f"AUC: {auc:.4f}")
# print(f"Classe 1 (fraude) - Précision: {precision:.4f}")
# print(f"Classe 1 (fraude) - Rappel:    {recall:.4f}")
# print(f"Classe 1 (fraude) - F1-score:  {f1:.4f}")
# print("Matrice de confusion :")
# print(conf_matrix)
# print("=" * 80)

# # Save metrics to file
# with open("metrics.txt", "w") as f:
#     f.write(f"Accuracy: {accuracy:.4f}\n")
#     f.write(f"AUC: {auc:.4f}\n")
#     f.write(f"Classe 1 (fraude) - Précision: {precision:.4f}\n")
#     f.write(f"Classe 1 (fraude) - Rappel:    {recall:.4f}\n")
#     f.write(f"Classe 1 (fraude) - F1-score:  {f1:.4f}\n")
#     f.write("Matrice de confusion :\n")
#     f.write(str(conf_matrix))

# # # Courbe ROC
# # probas_and_labels = predictions.select("probability", "label_index") \
# #     .rdd.map(lambda r: (float(r["probability"][1]), float(r["label_index"])))

# # bc_metrics = BinaryClassificationMetrics(probas_and_labels)

# # roc_points = bc_metrics.roc().toDF().collect()
# # x = [row[0] for row in roc_points]
# # y = [row[1] for row in roc_points]

# # plt.figure()
# # plt.plot(x, y, label="ROC Curve")
# # plt.xlabel("False Positive Rate")
# # plt.ylabel("True Positive Rate")
# # plt.title("ROC Curve - Random Forest")
# # plt.grid(True)
# # plt.legend()
# # plt.savefig("roc_curve.png")

# # Save model
# model.write().overwrite().save("hdfs://hadoop-master:9000/root/tp_fraude/models/rf_model")

# spark.stop()
