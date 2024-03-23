select 
(date_trunc('week', sales_date) + interval '6 days')::date endofweek,
sum(is_sales)::numeric is_sales from {{ref("sales_t1")}}
group by endofweek
order by endofweek desc