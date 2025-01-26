{{config(materialized = "table")}}
select 
beladungort,
zustellungort,
count(*) pair_count
from {{ref("stg_transport_data")}}
group by beladungort,zustellungort
order by pair_count desc