{{
  config(
    materialized = 'view'
  )
}}

WITH source AS (
    -- Load raw data from the ecommerce source table
    SELECT * FROM {{ source('ecom_raw', 'ecom_events') }}
),

cleaned AS (
    SELECT
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session,

        -- handle nulls
        COALESCE(user_session, 'unknown_session') AS coalesced_user_session,
        COALESCE(event_type, 'unknown_event_type') AS coalesced_event_type,
        COALESCE(category_code, 'N/A') AS coalesced_category_code,
        COALESCE(brand, 'Unknown') AS coalesced_brand,
        COALESCE(price, 0.0) AS coalesced_price

    FROM source
    WHERE
        user_id IS NOT NULL
        AND product_id IS NOT NULL
        AND event_time IS NOT NULL
        
        AND user_id IS NOT NULL
        AND product_id IS NOT NULL
)

SELECT
    -- IDs and casting
    CAST(user_id AS INT64) AS user_id,
    CAST(product_id AS INT64) AS product_id,
    CAST(category_id AS NUMERIC) AS category_id,
    CAST(coalesced_user_session AS STRING) AS user_session_id,

    -- Event details
    CAST(event_time AS TIMESTAMP) AS event_at_utc,
    CAST(coalesced_event_type AS STRING) AS event_type,

    -- Product details
    CAST(coalesced_category_code AS STRING) AS category_code,
    CAST(coalesced_brand AS STRING) AS product_brand,
    CAST(coalesced_price AS FLOAT64) AS event_price,

    -- Surrogate key for the event
    {{ dbt_utils.generate_surrogate_key([
        'user_session',
        'event_time',
        'event_type',
        'product_id',
        'user_id'
    ]) }} AS event_sk

FROM cleaned
QUALIFY row_number() OVER (
    PARTITION BY user_session, event_time, event_type, product_id, user_id
    ORDER BY event_time
) = 1