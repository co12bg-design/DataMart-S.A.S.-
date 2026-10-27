-- Esquema analítico. Se ejecuta automáticamente al crear el volumen del warehouse.
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key BIGSERIAL PRIMARY KEY,
    product_code TEXT NOT NULL UNIQUE,
    canonical_description TEXT,
    category TEXT NOT NULL DEFAULT 'Uncategorized',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.fact_transactions (
    transaction_key TEXT PRIMARY KEY, -- hash estable para cargas idempotentes
    invoice_no TEXT NOT NULL,
    product_code TEXT NOT NULL REFERENCES analytics.dim_product(product_code),
    description TEXT,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    transaction_ts TIMESTAMPTZ NOT NULL,
    customer_id TEXT,
    country TEXT NOT NULL DEFAULT 'Unknown',
    source_name TEXT NOT NULL,
    transaction_type TEXT NOT NULL CHECK (transaction_type IN ('SALE', 'RETURN')),
    gross_revenue NUMERIC(14, 4) NOT NULL,
    return_amount NUMERIC(14, 4) NOT NULL DEFAULT 0,
    net_revenue NUMERIC(14, 4) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_fact_transaction_ts ON analytics.fact_transactions(transaction_ts);
CREATE INDEX IF NOT EXISTS ix_fact_product_code ON analytics.fact_transactions(product_code);
CREATE INDEX IF NOT EXISTS ix_fact_country ON analytics.fact_transactions(country);

CREATE TABLE IF NOT EXISTS analytics.rejected_records (
    rejection_key TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    record_payload JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_rejected_source ON analytics.rejected_records(source_name);

CREATE OR REPLACE VIEW analytics.v_daily_product_revenue AS
SELECT product_code, transaction_ts::date AS transaction_date,
       SUM(gross_revenue) FILTER (WHERE transaction_type = 'SALE') AS gross_sales,
       SUM(return_amount) AS returns,
       SUM(net_revenue) AS net_revenue
FROM analytics.fact_transactions
GROUP BY product_code, transaction_ts::date;
