select
    country_code,
    bidding_zone,
    date(time_utc) as load_date,
    avg(load_mw) as avg_load_mw,
    max(load_mw) as peak_load_mw,
    min(load_mw) as min_load_mw,
    count(*) as hourly_points
from {{ ref('stg_energy_load_clean') }}
group by 1, 2, 3
