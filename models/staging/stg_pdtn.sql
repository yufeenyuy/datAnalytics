select 
"Date"::date production_date,
nullif("Banana 50 ",'NaN')::numeric banana_50,
nullif("Banana 100",'NaN')::numeric banana_100,
nullif("Square 50",'NaN')::numeric square_50,
nullif("Local 50",'NaN')::numeric local_50,
nullif("Local 100",'NaN')::numeric local_100,
nullif("Local 200",'NaN')::numeric local_200,
nullif("Local 250",'NaN')::numeric local_250,
nullif("Local 300",'NaN')::numeric local_300,
nullif("Special 100",'NaN')::numeric special_100,
nullif("Special 150",'NaN')::numeric special_150,
nullif("Special 200",'NaN')::numeric special_200,
nullif("Special 400",'NaN')::numeric special_400,
nullif("Special 500",'NaN')::numeric special_500,
nullif("Special 800",'NaN')::numeric special_800 
from
(select *,
row_number() over(partition by "Date" order by "Date" desc)::integer rownum
from {{source("confidts","production_data")}}
where loaded_at = (select max(loaded_at) from {{source("confidts","production_data")}})) pdtn
where rownum = 1