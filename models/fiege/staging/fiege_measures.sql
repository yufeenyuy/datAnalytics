{{config(materialized="table")}}

select 
count(*) anzahl_zustellung,
min(auftragseingang)::date min_auftragseingang,
max(auftragseingang)::date max_auftragseingang,
min(zustellung)::date min_zustellung,
max(zustellung)::date max_zustellung,
min(gewicht) min_gewicht,
max(gewicht) max_gewicht,
avg(gewicht) avg_gewicht,
min(prozess_zeit)::numeric min_prozesszeit,
avg(prozess_zeit)::numeric avg_prozesszeit,
max(prozess_zeit)::numeric max_prozesszeit
from {{ref("stg_transport_data")}}