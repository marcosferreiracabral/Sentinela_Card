"""Spark SQL analytical queries for audit, backtesting, and fraud detection metrics.

Registered temporary views expected in Spark catalog:
  - raw: raw input transactions
  - enriched: enriched transactions with calculated features and scores
  - alerts: flagged transactions (review/block)
  - fraud_labels: ground truth labels from synthetic dataset generation
"""

RANKING_CLIENTS_BY_ALERTS = """
SELECT
    customer_id,
    COUNT(1) AS total_alerts,
    SUM(CASE WHEN risk_level = 'block' THEN 1 ELSE 0 END) AS block_count,
    SUM(CASE WHEN risk_level = 'review' THEN 1 ELSE 0 END) AS review_count,
    ROUND(SUM(amount), 2) AS total_suspicious_amount,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    MAX(risk_score) AS max_risk_score
FROM alerts
GROUP BY customer_id
ORDER BY total_alerts DESC, total_suspicious_amount DESC
LIMIT 20
"""

FRAUD_RATE_BY_CATEGORY = """
SELECT
    e.merchant_category,
    COUNT(1) AS total_transactions,
    SUM(CASE WHEN e.risk_level = 'block' THEN 1 ELSE 0 END) AS block_count,
    SUM(CASE WHEN e.risk_level = 'review' THEN 1 ELSE 0 END) AS review_count,
    SUM(CASE WHEN e.risk_level IN ('review', 'block') THEN 1 ELSE 0 END) AS alert_count,
    ROUND(100.0 * SUM(CASE WHEN e.risk_level IN ('review', 'block') THEN 1 ELSE 0 END) / COUNT(1), 2) AS alert_rate_pct,
    ROUND(SUM(e.amount), 2) AS total_volume_brl,
    ROUND(SUM(CASE WHEN e.risk_level = 'block' THEN e.amount ELSE 0 END), 2) AS blocked_volume_brl
FROM enriched e
GROUP BY e.merchant_category
ORDER BY alert_rate_pct DESC, total_transactions DESC
"""

RULE_EFFECTIVENESS = """
WITH exploded_rules AS (
    SELECT
        e.transaction_id,
        EXPLODE(e.triggered_rules) AS rule_name,
        e.risk_score,
        e.risk_level,
        COALESCE(l.is_fraud, false) AS is_ground_truth_fraud,
        COALESCE(l.fraud_type, 'none') AS ground_truth_type
    FROM enriched e
    LEFT JOIN fraud_labels l ON e.transaction_id = l.transaction_id
    WHERE e.triggered_rules IS NOT NULL AND SIZE(e.triggered_rules) > 0
)
SELECT
    rule_name,
    COUNT(1) AS times_triggered,
    SUM(CASE WHEN is_ground_truth_fraud = true THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN is_ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positives,
    ROUND(100.0 * SUM(CASE WHEN is_ground_truth_fraud = true THEN 1 ELSE 0 END) / COUNT(1), 2) AS precision_pct
FROM exploded_rules
GROUP BY rule_name
ORDER BY times_triggered DESC, precision_pct DESC
"""

FALSE_POSITIVES_BY_RULE = """
WITH exploded_rules AS (
    SELECT
        e.transaction_id,
        EXPLODE(e.triggered_rules) AS rule_name,
        COALESCE(l.is_fraud, false) AS is_ground_truth_fraud
    FROM enriched e
    LEFT JOIN fraud_labels l ON e.transaction_id = l.transaction_id
    WHERE e.triggered_rules IS NOT NULL AND SIZE(e.triggered_rules) > 0
),
normal_totals AS (
    SELECT COUNT(1) AS total_normal_txs
    FROM enriched e
    LEFT JOIN fraud_labels l ON e.transaction_id = l.transaction_id
    WHERE COALESCE(l.is_fraud, false) = false
)
SELECT
    r.rule_name,
    SUM(CASE WHEN r.is_ground_truth_fraud = false THEN 1 ELSE 0 END) AS false_positive_count,
    COUNT(1) AS total_rule_triggers,
    ROUND(100.0 * SUM(CASE WHEN r.is_ground_truth_fraud = false THEN 1 ELSE 0 END) / MAX(n.total_normal_txs), 3) AS false_positive_rate_on_normal_pct
FROM exploded_rules r
CROSS JOIN normal_totals n
GROUP BY r.rule_name
ORDER BY false_positive_count DESC
"""

VOLUME_BY_HOUR = """
SELECT
    HOUR(timestamp) AS hour_of_day,
    COUNT(1) AS total_transactions,
    SUM(CASE WHEN risk_level = 'approve' THEN 1 ELSE 0 END) AS approve_count,
    SUM(CASE WHEN risk_level = 'review' THEN 1 ELSE 0 END) AS review_count,
    SUM(CASE WHEN risk_level = 'block' THEN 1 ELSE 0 END) AS block_count,
    ROUND(AVG(risk_score), 2) AS avg_risk_score,
    ROUND(SUM(amount), 2) AS total_volume_brl
FROM enriched
GROUP BY HOUR(timestamp)
ORDER BY hour_of_day ASC
"""

THRESHOLD_COMPARISON = """
SELECT
    'Atual (30/70)' AS scenario,
    SUM(CASE WHEN risk_score < 30 THEN 1 ELSE 0 END) AS approve_count,
    SUM(CASE WHEN risk_score >= 30 AND risk_score < 70 THEN 1 ELSE 0 END) AS review_count,
    SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS block_count,
    ROUND(100.0 * SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) / COUNT(1), 2) AS block_pct,
    ROUND(SUM(CASE WHEN risk_score >= 70 THEN amount ELSE 0 END), 2) AS blocked_amount_brl
FROM enriched
UNION ALL
SELECT
    'Conservador (25/60)' AS scenario,
    SUM(CASE WHEN risk_score < 25 THEN 1 ELSE 0 END) AS approve_count,
    SUM(CASE WHEN risk_score >= 25 AND risk_score < 60 THEN 1 ELSE 0 END) AS review_count,
    SUM(CASE WHEN risk_score >= 60 THEN 1 ELSE 0 END) AS block_count,
    ROUND(100.0 * SUM(CASE WHEN risk_score >= 60 THEN 1 ELSE 0 END) / COUNT(1), 2) AS block_pct,
    ROUND(SUM(CASE WHEN risk_score >= 60 THEN amount ELSE 0 END), 2) AS blocked_amount_brl
FROM enriched
UNION ALL
SELECT
    'Agressivo (40/80)' AS scenario,
    SUM(CASE WHEN risk_score < 40 THEN 1 ELSE 0 END) AS approve_count,
    SUM(CASE WHEN risk_score >= 40 AND risk_score < 80 THEN 1 ELSE 0 END) AS review_count,
    SUM(CASE WHEN risk_score >= 80 THEN 1 ELSE 0 END) AS block_count,
    ROUND(100.0 * SUM(CASE WHEN risk_score >= 80 THEN 1 ELSE 0 END) / COUNT(1), 2) AS block_pct,
    ROUND(SUM(CASE WHEN risk_score >= 80 THEN amount ELSE 0 END), 2) AS blocked_amount_brl
FROM enriched
"""

QUERIES = {
    "ranking_clients_by_alerts": RANKING_CLIENTS_BY_ALERTS,
    "fraud_rate_by_category": FRAUD_RATE_BY_CATEGORY,
    "rule_effectiveness": RULE_EFFECTIVENESS,
    "false_positives_by_rule": FALSE_POSITIVES_BY_RULE,
    "volume_by_hour": VOLUME_BY_HOUR,
    "threshold_comparison": THRESHOLD_COMPARISON,
}
