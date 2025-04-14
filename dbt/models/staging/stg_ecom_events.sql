WITH source AS (
    -- Use the source defined in schema.yml
    SELECT * FROM {{ source('ecom_source', 'ecom_events') }}
),

renamed_casted as (

    select
        -- IDs and Keys
        cast(user_id as int64) as user_id,
        cast(product_id as int64) as product_id,
        cast(category_id as numeric) as category_id,
        cast(user_session as string) as user_session_id,

        -- Event Details
        cast(event_time as timestamp) as event_at_utc,
        cast(event_type as string) as event_type,

        -- Product Details
        cast(category_code as string) as category_code,
        cast(brand as string) as product_brand,
        cast(price as float64) as event_price,

        -- Add a unique key for the event (hash of identifying columns)
        -- This helps with testing and potential incremental models
        {{ dbt_utils.generate_surrogate_key([
            'user_session', 'event_time', 'event_type', 'product_id'
           ]) }} as event_sk

    from source
    where
        user_id is not null
        and product_id is not null
        and event_time is not null
)

select * from renamed_casted