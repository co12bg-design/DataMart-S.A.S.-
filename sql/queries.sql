-- 1. Evolución mensual de ventas netas (ventas menos devoluciones).
SELECT date_trunc('month', transaction_ts)::date AS month,
       SUM(net_revenue) AS net_sales
FROM analytics.fact_transactions
GROUP BY 1 ORDER BY 1;

-- 2a. Categorías con mayor revenue bruto de ventas.
SELECT p.category, SUM(f.gross_revenue) AS gross_sales,
       SUM(f.return_amount) AS returned_amount,
       ROUND(100.0 * SUM(f.return_amount) / NULLIF(SUM(f.gross_revenue), 0), 2) AS return_pct_of_sales
FROM analytics.fact_transactions f
JOIN analytics.dim_product p USING (product_code)
GROUP BY p.category ORDER BY gross_sales DESC;

-- 2b. Categorías con mayor proporción de devoluciones: importe devuelto / ventas brutas.
SELECT p.category, SUM(f.return_amount) AS returned_amount,
       SUM(f.gross_revenue) AS gross_sales,
       ROUND(100.0 * SUM(f.return_amount) / NULLIF(SUM(f.gross_revenue), 0), 2) AS return_rate_pct
FROM analytics.fact_transactions f JOIN analytics.dim_product p USING (product_code)
GROUP BY p.category HAVING SUM(f.gross_revenue) > 0 ORDER BY return_rate_pct DESC;

-- 3a. Diez productos con mayor revenue neto.
SELECT f.product_code, COALESCE(p.canonical_description, f.product_code) AS product,
       SUM(f.net_revenue) AS net_revenue
FROM analytics.fact_transactions f JOIN analytics.dim_product p USING (product_code)
GROUP BY f.product_code, p.canonical_description ORDER BY net_revenue DESC LIMIT 10;

-- 3b. Diez productos con mayor tasa de devolución por importe.
SELECT f.product_code, COALESCE(p.canonical_description, f.product_code) AS product,
       SUM(f.return_amount) AS returns, SUM(f.gross_revenue) AS gross_sales,
       ROUND(100.0 * SUM(f.return_amount) / NULLIF(SUM(f.gross_revenue), 0), 2) AS return_rate_pct
FROM analytics.fact_transactions f JOIN analytics.dim_product p USING (product_code)
GROUP BY f.product_code, p.canonical_description
HAVING SUM(f.gross_revenue) > 0 ORDER BY return_rate_pct DESC, returns DESC LIMIT 10;

-- 4. Países con más transacciones y ticket promedio por factura.
WITH invoice_totals AS (
  SELECT country, invoice_no, SUM(net_revenue) AS invoice_net
  FROM analytics.fact_transactions GROUP BY country, invoice_no
)
SELECT country, COUNT(*) AS invoices, AVG(invoice_net) AS average_net_ticket,
       SUM(invoice_net) AS total_net_revenue
FROM invoice_totals GROUP BY country ORDER BY invoices DESC;

-- 5. Clientes identificados frente a transacciones sin Customer ID.
SELECT CASE WHEN customer_id IS NULL THEN 'ANONYMOUS' ELSE 'IDENTIFIED' END AS customer_group,
       COUNT(*) AS lines, COUNT(DISTINCT invoice_no) AS invoices,
       AVG(net_revenue) AS average_line_net_revenue, SUM(net_revenue) AS total_net_revenue
FROM analytics.fact_transactions GROUP BY 1;

-- 6a. Códigos con más de una descripción observada (diferencias de mayúsculas incluidas).
SELECT product_code, COUNT(DISTINCT TRIM(description)) AS observed_description_count,
       ARRAY_AGG(DISTINCT description ORDER BY description) AS observed_descriptions
FROM analytics.fact_transactions WHERE description IS NOT NULL
GROUP BY product_code HAVING COUNT(DISTINCT TRIM(description)) > 1
ORDER BY observed_description_count DESC, product_code;

-- 6b. Cantidad total de códigos únicos de producto.
SELECT COUNT(DISTINCT product_code) AS unique_product_codes FROM analytics.fact_transactions;

-- 7. Diagnóstico para recomendaciones: productos con más devoluciones y revenue neto.
SELECT product_code, MAX(description) AS example_description,
       SUM(gross_revenue) AS gross_sales, SUM(return_amount) AS returns,
       SUM(net_revenue) AS net_revenue,
       ROUND(100.0 * SUM(return_amount) / NULLIF(SUM(gross_revenue), 0), 2) AS return_rate_pct
FROM analytics.fact_transactions GROUP BY product_code
HAVING SUM(gross_revenue) > 0 ORDER BY return_rate_pct DESC, returns DESC LIMIT 20;
