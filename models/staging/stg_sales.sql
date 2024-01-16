select * from {{source("confidts","Sales_Data")}}
where loaded_at::date = (select max(loaded_at::date) from {{source("confidts","Sales_Data")}})