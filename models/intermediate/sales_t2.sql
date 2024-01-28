
select 
sales_date,
should_sales,
lag(should_sales,1) over(order by sales_date)::numeric lagged_should_sales,
should_sales - lag(should_sales,1) over(order by sales_date)::numeric lagged_should_sales_dif,
is_sales,
lag(is_sales,1) over(order by sales_date)::numeric lagged_is_sales,
is_sales - lag(is_sales,1) over(order by sales_date)::numeric lagged_is_sales_dif,
unsold,
unsold_damaged
from
(select sales_date,
sum(should_sales) should_sales,
sum(is_sales) is_sales,
sum(unsold) unsold,
sum(unsold_damaged) unsold_damaged
from {{ref("sales_t1")}}
group by 1) t