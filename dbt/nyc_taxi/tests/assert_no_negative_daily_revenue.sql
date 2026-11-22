-- Fails if any day has negative total revenue (returns the offending rows).
select pickup_date, total_revenue
from {{ ref('fct_daily_trips') }}
where total_revenue < 0
