select 
"Date"::date production_date,
"Banana 50 "::numeric banana_50,
"Banana 100"::numeric banana_100,
"Square 50"::numeric square_50,
"Local 50"::numeric local_50,
"Local 100"::numeric local_100,
"Local 200"::numeric local_200,
"Local 250"::numeric local_250,
"Local 300"::numeric local_300,
"Special 100"::numeric special_100,
"Special 150"::numeric special_150,
"Special 200"::numeric special_200,
"Special 400"::numeric special_400,
"Special 500"::numeric special_500,
"Special 800"::numeric special_800 
from
(select *,
row_number() over(partition by "Date" order by "Date" desc)::integer rownum
from {{source("confidts","Production_Data")}}
where loaded_at = (select max(loaded_at) from {{source("confidts","Production_Data")}})) pdtn
where rownum = 1