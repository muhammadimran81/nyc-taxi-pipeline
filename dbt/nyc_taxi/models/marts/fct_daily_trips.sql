-- One row per pickup day: volume, revenue and trip averages.
select
    pickup_date,
    count(*)                         as trips,
    sum(total_amount)                as total_revenue,
    round(avg(fare_amount), 2)       as avg_fare,
    round(avg(trip_distance), 2)     as avg_distance_miles,
    round(avg(trip_minutes), 1)      as avg_trip_minutes,
    round(100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0), 1) as tip_pct_of_fare
from {{ ref('stg_yellow_trips') }}
group by pickup_date
