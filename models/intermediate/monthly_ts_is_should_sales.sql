select 
(date_trunc('month', sales_date) + interval '1 month' - interval '1 days')::date endofmonth,
sum(should_sales)::numeric should_sales,
sum(is_sales)::numeric is_sales 
from {{ref("sales_t1")}}
group by endofmonth
order by endofmonth desc