WITH stg_events AS (
    SELECT * FROM {{ ref('stg_ecom_events') }}
)

SELECT DISTINCT
    user_id,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_sk
FROM stg_events
WHERE user_id IS NOT NULL