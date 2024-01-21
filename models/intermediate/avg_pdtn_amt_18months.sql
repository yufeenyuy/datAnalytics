select avg(amount) periodsavg from {{ref("pdtn_t1")}}
where (date_trunc('month',production_date))::date >= 
(select date_trunc('month',max(production_date)-interval '18 months')::date from {{ref("pdtn_t1")}})
