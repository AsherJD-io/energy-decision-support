select
    timestamp(time_utc) as time_utc,
    country_code,
    bidding_zone,
    cast(load_mw as float64) as load_mw,
    source,
    timestamp(ingested_at) as ingested_at
from `energy-dss-1773915785.energy_dss.energy_load_clean`
