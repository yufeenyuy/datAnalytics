select 
production_date,
fuel,
lag(fuel, 1) over(order by production_date)::numeric flour_lagged
from
(select 
production_date, 
fuel,
row_number() over(partition by production_date)::integer rownum 
from 
(select 
"Date"::date production_date,
"Fuel in cfa"::numeric fuel
from {{source("confidts","production_data")}}
where loaded_at = (select max(loaded_at) from {{source("confidts","production_data")}})) it 
where production_date not in ('1900-01-28','7030-01-26')) it1 
where rownum = 1
order by production_date asc