select 
currency,
rate
from
(select currency, rate,
row_number() over(partition by currency order by rate asc) rownum
from
(select
split_part(currency,' ',2)::text currency,
"value"::numeric rate
from
(select * 
from {{ref("stg_cfa_exchangerate")}}
where date_trunc('second',loaded_at::date) = (select max(date_trunc('second',loaded_at::date)) from {{ref("stg_cfa_exchangerate")}})) ex)
ex1) ex2
where rownum = 1
order by rate desc
