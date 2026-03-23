select
    assertion_name,
    status,
    cast(observed_value as int64) as observed_value,
    rule
from `energy-dss-1773915785.energy_dss.dq_assertions`
