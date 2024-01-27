select * from {{source("confidts","sales_data")}}
where loaded_at::date = (select max(loaded_at::date) from {{source("confidts","sales_data")}})