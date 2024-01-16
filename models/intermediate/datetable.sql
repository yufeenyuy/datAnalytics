
select 
"date",
(date_trunc('month', "date")::date + interval '1 month' - interval '1 day')::date date_eom,
to_char("date",'Mon-YYYY') date_my,
to_char("date",'YYYYMM')::integer date_my_reorder,
to_char("date",'DD-Mon') date_dm,
to_char("date",'YYYYMMDD')::integer date_dm_reorder,
to_char("date",'YYYY')::integer date_y,
to_char("date", 'Mon') date_m,
to_char("date",'DD') date_dnum,
to_char("date",'Dy') date_day,
to_char("date",'W')  date_month_wiknum,
to_char("date",'WW') date_wiknum
from
(select distinct production_date "date" from {{ref("pdtn_t1")}}
union
select distinct sales_date "date" from {{ref("sales_t1")}}) dt