select
    trip_month,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_at::date                                         as pickup_date,
    round(extract(epoch from dropoff_at - pickup_at) / 60.0, 1) as trip_minutes,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        else 'Other/Unknown'
    end                                                     as payment_method,
    fare_amount,
    tip_amount,
    total_amount
from {{ source('raw', 'yellow_trips') }}
