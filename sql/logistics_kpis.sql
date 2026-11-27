-- Databricks SQL KPI Views for Logistics & Delivery Performance Analytics
-- Parameterized with :catalog and :schema so dev (workspace.logistics_dev) and
-- prod (workspace.logistics) deploy cleanly from the same SQL source.

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema);

-- 1. Executive KPI Summary
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_executive_summary') AS
SELECT
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN status <> 'In Transit' THEN 1 ELSE 0 END) AS completed_shipments,
    SUM(CASE WHEN status = 'In Transit' THEN 1 ELSE 0 END) AS in_transit_shipments,
    SUM(CASE WHEN status <> 'In Transit' AND actual_days <= promised_days THEN 1 ELSE 0 END) AS on_time_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN status <> 'In Transit' AND actual_days <= promised_days THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN status <> 'In Transit' THEN 1 ELSE 0 END), 0),
        2
    ) AS on_time_delivery_pct,
    ROUND(AVG(CASE WHEN status <> 'In Transit' THEN delay_days END), 2) AS avg_delay_days,
    ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(shipping_cost / NULLIF(distance_km, 0)), 4) AS avg_cost_per_km,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS return_rate_pct
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean');

-- 2. Late Delivery & Carrier Performance Scorecard
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_carrier_scorecard') AS
SELECT
    s.carrier,
    c.service_type,
    c.base_cost,
    c.contact_sla_hours,
    COUNT(*) AS shipments,
    SUM(CASE WHEN s.actual_days <= s.promised_days THEN 1 ELSE 0 END) AS on_time_shipments,
    SUM(CASE WHEN s.actual_days > s.promised_days THEN 1 ELSE 0 END) AS late_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN s.actual_days <= s.promised_days THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS on_time_pct,
    ROUND(
        100.0 * SUM(CASE WHEN s.actual_days > s.promised_days THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS late_delivery_pct,
    ROUND(AVG(s.delay_days), 2) AS avg_delay_days,
    ROUND(SUM(s.shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(s.shipping_cost / NULLIF(s.distance_km, 0)), 2) AS avg_cost_per_km,
    ROUND(
        100.0 * SUM(CASE WHEN s.status = 'Returned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS return_rate_pct
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean') AS s
INNER JOIN IDENTIFIER(:catalog || '.' || :schema || '.carriers') AS c
    ON s.carrier = c.carrier
WHERE s.status <> 'In Transit'
GROUP BY s.carrier, c.service_type, c.base_cost, c.contact_sla_hours
ORDER BY on_time_pct DESC;

-- 3. Warehouse Performance Scorecard
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_warehouse_performance') AS
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.region,
    w.manager,
    COUNT(*) AS shipments,
    SUM(CASE WHEN s.actual_days <= s.promised_days THEN 1 ELSE 0 END) AS on_time_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN s.actual_days <= s.promised_days THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS on_time_pct,
    ROUND(AVG(s.delay_days), 2) AS avg_delay_days,
    ROUND(SUM(s.shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(s.shipping_cost / NULLIF(s.distance_km, 0)), 2) AS avg_cost_per_km,
    ROUND(
        100.0 * SUM(CASE WHEN s.status = 'Returned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS return_rate_pct
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean') AS s
INNER JOIN IDENTIFIER(:catalog || '.' || :schema || '.warehouses') AS w
    ON s.warehouse_id = w.warehouse_id
WHERE s.status <> 'In Transit'
GROUP BY w.warehouse_id, w.warehouse_name, w.region, w.manager
ORDER BY avg_delay_days DESC, on_time_pct ASC;

-- 4. Destination City Performance
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_destination_performance') AS
SELECT
    destination_city,
    COUNT(*) AS shipments,
    SUM(CASE WHEN actual_days <= promised_days THEN 1 ELSE 0 END) AS on_time_shipments,
    SUM(CASE WHEN actual_days > promised_days THEN 1 ELSE 0 END) AS late_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN actual_days <= promised_days THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS on_time_pct,
    ROUND(AVG(delay_days), 2) AS avg_delay_days,
    MAX(delay_days) AS max_delay_days,
    ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(shipping_cost / NULLIF(distance_km, 0)), 2) AS avg_cost_per_km,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS return_rate_pct
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean')
WHERE status <> 'In Transit'
GROUP BY destination_city
ORDER BY avg_delay_days DESC;

-- 5. Top 10 Most Delayed Destinations
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_top10_delayed_destinations') AS
SELECT
    destination_city,
    COUNT(*) AS completed_shipments,
    SUM(CASE WHEN actual_days > promised_days THEN 1 ELSE 0 END) AS late_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN actual_days > promised_days THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS late_rate_pct,
    ROUND(AVG(delay_days), 2) AS avg_delay_days,
    MAX(delay_days) AS max_delay_days
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean')
WHERE status <> 'In Transit'
GROUP BY destination_city
ORDER BY avg_delay_days DESC, late_rate_pct DESC
LIMIT 10;

-- 6. Most Expensive Carriers per Kilometer
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_most_expensive_carriers') AS
SELECT
    s.carrier,
    c.service_type,
    c.base_cost,
    COUNT(*) AS shipments,
    ROUND(SUM(s.shipping_cost), 2) AS total_shipping_cost,
    ROUND(SUM(s.distance_km), 2) AS total_distance_km,
    ROUND(AVG(s.shipping_cost / NULLIF(s.distance_km, 0)), 4) AS avg_cost_per_km,
    ROUND(SUM(s.shipping_cost) / NULLIF(SUM(s.distance_km), 0), 4) AS weighted_cost_per_km
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean') AS s
INNER JOIN IDENTIFIER(:catalog || '.' || :schema || '.carriers') AS c
    ON s.carrier = c.carrier
GROUP BY s.carrier, c.service_type, c.base_cost
ORDER BY avg_cost_per_km DESC;

-- 7. Monthly Delivery & Cost Trend
CREATE OR REPLACE VIEW IDENTIFIER(:catalog || '.' || :schema || '.kpi_monthly_trend') AS
SELECT
    DATE_TRUNC('MONTH', CAST(order_date AS DATE)) AS order_month,
    COUNT(*) AS total_shipments,
    SUM(CASE WHEN status <> 'In Transit' THEN 1 ELSE 0 END) AS completed_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN status <> 'In Transit' AND actual_days <= promised_days THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN status <> 'In Transit' THEN 1 ELSE 0 END), 0),
        2
    ) AS on_time_pct,
    ROUND(AVG(CASE WHEN status <> 'In Transit' THEN delay_days END), 2) AS avg_delay_days,
    ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(shipping_cost / NULLIF(distance_km, 0)), 4) AS avg_cost_per_km,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS return_rate_pct
FROM IDENTIFIER(:catalog || '.' || :schema || '.shipments_clean')
GROUP BY DATE_TRUNC('MONTH', CAST(order_date AS DATE))
ORDER BY order_month;
