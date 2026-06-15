CREATE OR REPLACE VIEW gold.v_revenue_by_category AS
SELECT
    category,
    COUNT(DISTINCT order_id)                              AS total_orders,
    ROUND(SUM(order_total)::NUMERIC, 2)                   AS total_revenue,
    ROUND(AVG(order_total)::NUMERIC, 2)                   AS avg_ticket,
    ROUND(
        SUM(order_total) * 100.0
        / SUM(SUM(order_total)) OVER ()
    , 2)                                                  AS revenue_share_pct
FROM gold.orders_silver
WHERE status = 'completed'
GROUP BY category
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW gold.v_top_customers AS
SELECT
    customer_id,
    COUNT(DISTINCT order_id)               AS total_orders,
    ROUND(SUM(order_total)::NUMERIC, 2)    AS lifetime_value,
    ROUND(AVG(order_total)::NUMERIC, 2)    AS avg_ticket,
    MAX(order_date)                        AS last_order_date,
    MIN(order_date)                        AS first_order_date
FROM gold.orders_silver
WHERE status = 'completed'
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 10;

-- Regra: HIGH > 60 dias | MEDIUM 31-60 dias | LOW <= 30 dias
CREATE OR REPLACE VIEW gold.v_churn_risk_score AS
WITH customer_activity AS (
    SELECT
        customer_id,
        MAX(order_date)                                    AS last_order_date,
        COUNT(DISTINCT order_id)                           AS total_orders,
        ROUND(SUM(order_total)::NUMERIC, 2)                AS lifetime_value,
        CURRENT_DATE - MAX(order_date)                     AS days_since_last_order
    FROM gold.orders_silver
    WHERE status = 'completed'
    GROUP BY customer_id
)
SELECT
    customer_id,
    last_order_date,
    total_orders,
    lifetime_value,
    days_since_last_order,
    CASE
        WHEN days_since_last_order > 60 THEN 'HIGH'
        WHEN days_since_last_order > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                    AS churn_risk
FROM customer_activity
ORDER BY days_since_last_order DESC;

CREATE OR REPLACE VIEW gold.v_daily_revenue AS
SELECT
    order_date,
    COUNT(DISTINCT order_id)               AS orders_count,
    ROUND(SUM(order_total)::NUMERIC, 2)    AS daily_revenue
FROM gold.orders_silver
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date;
