-- One row per month and pickup zone: where the money is made.
select
    t.trip_month,
    t.pickup_location_id,
    coalesce(z.borough, 'Unknown')   as borough,
    coalesce(z.zone, 'Unknown')      as zone,
    count(*)                         as trips,
    sum(t.total_amount)              as total_revenue,
    round(avg(t.total_amount), 2)    as avg_revenue_per_trip
from {{ ref('stg_yellow_trips') }} t
left join {{ source('raw', 'taxi_zones') }} z
    on z.location_id = t.pickup_location_id
group by 1, 2, 3, 4
