select 
(date_trunc('week', sales_date) + interval '6 days')::date endofweek,
sum(should_sales)::numeric should_sales 
from {{ref("sales_t1")}}
group by endofweek