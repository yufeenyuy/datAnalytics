select 
production_date,
(date_trunc('month',production_date) + interval '1 month' - interval '1 day')::date production_eom,
date_part('year', production_date) production_year,
to_char(production_date, 'Mon') production_month, 
to_char(production_date, 'Mon-yyyy') production_my,
date_part('day',production_date) production_md,
to_char(production_date, 'DD-Mon') production_dm,
replace(bread_type,'_','') bread_type,
split_part(bread_type,'_',2)::integer unitprice,
nullif(amount,'NaN')::numeric amount
from
(select 
    "production_date",
    unnest(array['banana_50','banana_100','square_50','local_50','local_100','local_200','local_250',
    'local_300','special_100','special_150','special_200','special_400','special_500','special_800'])::text as bread_type,

    unnest(array[banana_50, banana_100, square_50, local_50, local_100, local_200, local_250,
    local_300, special_100, special_150, special_200, special_400, special_500, special_800])::numeric as amount
from {{ref("stg_pdtn")}}) t1
where production_date not in ('1900-01-28','7030-01-26')
order by production_date asc