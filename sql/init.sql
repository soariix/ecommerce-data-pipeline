-- ============================================================
-- init.sql — Inicialização do schema Gold no PostgreSQL
-- Executado automaticamente pelo docker-entrypoint-initdb.d
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- Tabela staging da camada Silver (populada via Spark JDBC)
CREATE TABLE IF NOT EXISTS gold.orders_silver (
    order_id             INTEGER     PRIMARY KEY,
    customer_id          INTEGER     NOT NULL,
    product_id           INTEGER     NOT NULL,
    quantity             INTEGER,
    price                NUMERIC(10, 2),
    status               VARCHAR(20),
    order_date           DATE,
    payment_method       VARCHAR(30),
    order_total          NUMERIC(10, 2),
    city                 VARCHAR(100),
    state                CHAR(2),
    signup_date          DATE,
    last_purchase_date   DATE,
    product_name         VARCHAR(200),
    category             VARCHAR(50),
    unit_price           NUMERIC(10, 2)
);

-- Tabelas Gold (populadas via Spark JDBC / gold_views.sql)
CREATE TABLE IF NOT EXISTS gold.revenue_by_category (
    category          VARCHAR(50)   PRIMARY KEY,
    total_orders      INTEGER,
    total_revenue     NUMERIC(12, 2),
    avg_ticket        NUMERIC(10, 2),
    revenue_share_pct NUMERIC(5, 2)
);

CREATE TABLE IF NOT EXISTS gold.top_customers (
    customer_id      INTEGER       PRIMARY KEY,
    total_orders     INTEGER,
    lifetime_value   NUMERIC(12, 2),
    avg_ticket       NUMERIC(10, 2),
    last_order_date  DATE,
    first_order_date DATE
);

CREATE TABLE IF NOT EXISTS gold.churn_risk_score (
    customer_id           INTEGER       PRIMARY KEY,
    last_order_date       DATE,
    total_orders          INTEGER,
    lifetime_value        NUMERIC(12, 2),
    days_since_last_order INTEGER,
    churn_risk            VARCHAR(10)
);
