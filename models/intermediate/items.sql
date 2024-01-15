select 
production_date,
flour,
lag(flour, 1) over(order by production_date)::numeric flour_lagged
from
(select 
production_date, 
flour,
row_number() over(partition by production_date)::integer rownum 
from 
(select 
"Date"::date production_date,
"Flour in bags"::numeric flour
from {{source("confidts","Production_Data")}}
where loaded_at = (select max(loaded_at) from {{source("confidts","Production_Data")}})) it 
where production_date not in ('1900-01-28','7030-01-26')) it1 
where rownum = 1