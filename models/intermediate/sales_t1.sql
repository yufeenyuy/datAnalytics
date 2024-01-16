select 
coalesce(nullif("Date",'NaT')::date,'2023-01-03')::date sales_date,
case 
   when "Names" like '_onstantine' then 'constantine'
   when "Names" like '_brahi_' then 'ibrahim'
   when "Names" ~~ 'Ubale' then 'ubale'
   when "Names" = 'Pavert' then 'pavert'
   when "Names" = 'Paul & Ubale' or "Names" = 'paul & Ubale' then 'paul & ubale'
   when "Names" = 'Unknown' or "Names" = 'unknown' then 'unknown'
   else "Names" end worker_name, 
"Bread Command"::numeric should_sales,
"Sold"::numeric is_sales,
"Return"::numeric unsold,
"Losses"::numeric unsold_damaged         
from {{ref("stg_sales")}}