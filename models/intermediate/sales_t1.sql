select 
coalesce(nullif("Date",'NaT')::date,
(select (max(nullif("Date",'NaT')::date) + interval '1 day')::date from {{ref("stg_sales")}}))::date sales_date,
case 
   when "Names" like '_onstantine' then 'constantine'
   when "Names" like '_brahi_' then 'ibrahim'
   when "Names" ~~ 'Ubale' then 'ubale'
   when "Names" = 'Pavert' then 'pavert'
   when "Names" = 'Paul & Ubale' or "Names" = 'paul & Ubale' then 'paul & ubale'
   when "Names" = 'Unknown' or "Names" = 'unknown' then 'unknown'
   else "Names" end worker_name, 
nullif("Bread Command"::numeric,'NaN')::numeric should_sales,
nullif("Sold"::numeric,'NaN')::numeric is_sales,
nullif("Return"::numeric,'NaN')::numeric unsold,
nullif("Losses"::numeric,'NaN')::numeric unsold_damaged         
from {{ref("stg_sales")}}