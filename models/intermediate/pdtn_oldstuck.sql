select 
production_date,
old_stuck,
lag(old_stuck,1) over(order by production_date)::numeric old_stuck_lagged
from
(select 
production_date,
old_stuck,
row_number() over(partition by production_date order by production_date desc)::integer rownum 
from
(select 
"Date"::date production_date,
"Old stuck"::numeric old_stuck
from {{source("confidts","Production_Data")}}
where loaded_at = (select max(loaded_at) from {{source("confidts","Production_Data")}})) b
where production_date not in ('1900-01-28','7030-01-26')) b1 
where rownum = 1