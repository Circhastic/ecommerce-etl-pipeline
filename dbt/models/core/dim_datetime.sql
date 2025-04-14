with event_times as (
    select distinct
       timestamp_trunc(event_at_utc, hour) as datetime_hour_utc
    from {{ ref('stg_ecom_events') }}
)

select
    datetime_hour_utc,
    {{ dbt_utils.generate_surrogate_key(['datetime_hour_utc']) }} as datetime_sk,
    extract(date from datetime_hour_utc) as date_utc,
    extract(year from datetime_hour_utc) as year_utc,
    extract(month from datetime_hour_utc) as month_utc,
    extract(day from datetime_hour_utc) as day_utc,
    extract(hour from datetime_hour_utc) as hour_utc,
    format_date('%A', date(datetime_hour_utc)) as day_name_utc, -- Full day name (e.g., Monday)
    format_date('%a', date(datetime_hour_utc)) as day_name_short_utc, -- Short day name (e.g., Mon)
    extract(dayofweek from datetime_hour_utc) as day_of_week_utc, -- Sunday=1, Saturday=7
    extract(dayofyear from datetime_hour_utc) as day_of_year_utc,
    extract(week from datetime_hour_utc) as week_of_year_utc,
    extract(quarter from datetime_hour_utc) as quarter_of_year_utc,
    format_timestamp('%Y-%m', datetime_hour_utc) as year_month_utc

from event_times
order by datetime_hour_utc