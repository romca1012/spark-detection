from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql import Row

app = FastAPI()

# Initialiser Spark et charger le modèle
spark = SparkSession.builder \
    .appName("FraudDetectionAPI") \
    .getOrCreate()

model = PipelineModel.load("models/best_model")

# Définir le schéma d’entrée
class Transaction(BaseModel):
    amount: float
    credit_score: float
    num_credit_cards: float
    num_cards_issued: float
    latitude: float
    longitude: float
    use_chip: str
    card_brand: str
    card_type: str
    has_chip: str
    gender: str

def explain_decision(tx: Transaction):
    reasons = []

    if tx.amount > 2000:
        reasons.append("Montant élevé")
    if tx.credit_score < 400:
        reasons.append("Score de crédit faible")
    if tx.latitude > 50 or tx.latitude < 35:
        reasons.append("Localisation inhabituelle")
    if tx.use_chip.lower() == "no":
        reasons.append("Transaction sans puce")
    if tx.num_cards_issued > 5:
        reasons.append("Beaucoup de cartes émises")
    if tx.num_credit_cards > 8:
        reasons.append("Trop de cartes de crédit")
    if tx.gender.lower() not in ["male", "female"]:
        reasons.append("Genre non identifié")

    return reasons or ["Aucune anomalie détectée dans les règles simples."]

@app.post("/predict")
def predict(tx: Transaction):
    # Transformer en DataFrame Spark
    row = Row(**tx.dict())
    df = spark.createDataFrame([row])

    # Prédire avec Spark ML
    prediction = model.transform(df)
    pred = prediction.select("prediction", "probability").first()

    prediction_label = "FRAUDE" if pred["prediction"] == 1.0 else "TRANSACTION NORMALE"
    explanation = explain_decision(tx)

    return {
        "prediction": prediction_label,
        "fraude_probabilite": round(float(pred["probability"][1]), 4),
        "explication": explanation
    }

@app.get("/metrics")
def get_metrics():
    try:
        with open("metrics_optimized.txt", "r") as f:
            content = f.read()
        return {"metrics": content}
    except FileNotFoundError:
        return {"error": "metrics.txt not found"}
