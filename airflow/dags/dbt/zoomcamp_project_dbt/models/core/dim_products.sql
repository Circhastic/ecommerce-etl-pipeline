WITH product_events AS (
    SELECT
        product_id,
        category_id,
        category_code,
        product_brand,
        event_at_utc
    FROM {{ ref('stg_ecom_events') }}
    WHERE product_id IS NOT NULL
),

latest_product_info AS (
    -- Find the latest record for each product to get the most recent attributes
    SELECT
        product_id,
        category_id,
        category_code,
        product_brand,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY event_at_utc DESC
        ) AS rn
    FROM product_events
)

SELECT
    product_id,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_sk,
    category_id,
    category_code,
    COALESCE(product_brand, 'Unknown') AS product_brand
FROM latest_product_info
WHERE rn = 1