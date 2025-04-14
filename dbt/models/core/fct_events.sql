{{
    config(
        materialized = "incremental",
        unique_key = "event_sk",
        incremental_strategy = "merge",
        partition_by = {
            "field": "event_at_utc",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["product_sk", "user_sk"]
    )
}}

with stg_events as (
    select * from {{ ref('stg_ecom_events') }}

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- fetch events occurred since the last run
    where event_at_utc > (select max(event_at_utc) from {{ this }})
    {% endif %}
),

dim_products as (
    select product_sk, product_id from {{ ref('dim_products') }}
),

dim_users as (
    select user_sk, user_id from {{ ref('dim_users') }}
),

dim_datetime as (
    select datetime_sk, datetime_hour_utc from {{ ref('dim_datetime') }}
)

select
    dp.product_sk,
    du.user_sk,
    dd.datetime_sk,
    se.event_type,
    se.user_session_id,
    se.event_price,
    se.event_at_utc,
    se.event_sk

from stg_events se
left join dim_products dp on se.product_id = dp.product_id
left join dim_users du on se.user_id = du.user_id
left join dim_datetime dd on timestamp_trunc(se.event_at_utc, hour) = dd.datetime_hour_utc
