-- 1. ✅ Taux de fraude global
CREATE OR REPLACE VIEW gold.v_taux_fraude_global AS
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN label_pred = 1 THEN 1 ELSE 0 END) AS total_fraudes,
    ROUND(100.0 * SUM(CASE WHEN label_pred = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS taux_fraude_percent
FROM gold.transactions_scored;

-- 2. 🗺️ Fraudes par ville/état
CREATE OR REPLACE VIEW gold.v_fraude_par_ville_etat AS
SELECT
    merchant_state,
    merchant_city,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN label_pred = 1 THEN 1 ELSE 0 END) AS nb_fraudes
FROM gold.transactions_scored
GROUP BY merchant_state, merchant_city;

-- 3. 💳 Montant moyen (fraude vs normal)
CREATE OR REPLACE VIEW gold.v_montant_moyen_par_type AS
SELECT
    label_pred AS est_fraude,
    COUNT(*) AS nb_trans,
    ROUND(AVG(amount)::numeric, 2) AS montant_moyen
FROM gold.transactions_scored
GROUP BY label_pred;

-- 4. 🔝 Top clients suspects
CREATE OR REPLACE VIEW gold.v_top_clients_suspects AS
SELECT
    client_id,
    COUNT(*) AS nb_trans,
    ROUND(AVG(fraud_probability)::numeric, 4) AS moyenne_proba,
    SUM(CASE WHEN label_pred = 1 THEN 1 ELSE 0 END) AS nb_fraudes
FROM gold.transactions_scored
GROUP BY client_id
ORDER BY moyenne_proba DESC
LIMIT 100;
