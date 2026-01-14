-- Small, stable, and representative dataset for demo and presentation.
-- Filters by fixed event_date to ensure consistency and performance.
-- Safe to connect to BI tools without performance or privacy risks.
SELECT
    event_time,
    user_id,
    page_domain AS website_section,
    page_path,
    device_type,
    traffic_channel,
    CASE WHEN is_download = 1 THEN 'Yes' ELSE 'No' END AS was_download,
    CASE WHEN is_link_click = 1 THEN 'Yes' ELSE 'No' END AS was_link_click,
    product_price,
    ecommerce_order_id
FROM {{ ref('fcts_clicks') }}
WHERE
    event_date = CAST('2013-07-15' AS Date)
ORDER BY event_time
LIMIT 1000
