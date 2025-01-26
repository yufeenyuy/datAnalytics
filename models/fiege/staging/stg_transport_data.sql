{{config(materialized = "table")}}

select 
auftragsgeber,
auftragnummer,
prozess::bigint prozess,
beladunglkz,
beladungort,
zustellunglkz,
zustellungort,
gewicht,
stellplaetze,
auftragseingang,
(date_trunc('month',auftragseingang) + interval '1 month' - interval '1 day')::date auftragnummer_eom,
to_char(auftragseingang,'YYYY')::text auftragseingang_y,
to_char(auftragseingang,'YYYY-Mon')::text auftragseingang_ym,
to_char(auftragseingang,'YYYYMM')::text auftragseingang_ym_reorder,
to_char(auftragseingang,'Mon-DD')::text auftragseingang_md,
to_char(auftragseingang,'MMDD')::text auftragseingang_md_reorder,
to_char(auftragseingang, 'Dy')::text auftragseingang_dow,
extract('ISODOW'from auftragseingang)::text auftragseingang_dow_nr,
extract('Hour' from auftragseingang)::text auftragseingang_hr,
zustellung,
(date_trunc('month',zustellung) + interval '1 month' - interval '1 day')::date zustellung_eom,
to_char(zustellung,'YYYY')::text zustellung_y,
to_char(zustellung,'YYYY-Mon')::text zustellung_ym,
to_char(zustellung,'YYYYMM')::text zustellung_ym_reorder,
to_char(zustellung,'Mon-DD')::text zustellung_md,
to_char(zustellung,'MMDD')::text zustellung_md_reorder,
to_char(zustellung, 'Dy')::text zustellung_dow,
extract('ISODOW'from zustellung)::text zustellung_dow_nr,
extract('Hour' from zustellung)::text zustellung_hr,
(zustellung::date - auftragseingang::date)::integer prozess_zeit
from
(select 
nullif(auftraggebernr,'nan')::text auftragsgeber,
nullif(auftragsnummer,'nan')::text auftragnummer,
replace(nullif(prozess,'nan'),'.0','')::integer prozess,
nullif(beladunglkz,'nan')::text beladunglkz,
nullif(beladungort,'nan')::text beladungort,
nullif(zustellunglkz,'nan')::text zustellunglkz,
nullif(zustellungort,'nun')::text zustellungort,
to_timestamp(nullif(auftragseingang,'nan')::text,'DD-MM-YYYY HH24:MI:SS') at time zone 'Europe/Berlin' auftragseingang,
to_timestamp(nullif("zustellung / prozessabschluß",'nan')::text, 'DD-MM-YYYY HH24:MI:SS') at time zone 'Europe/Berlin' zustellung,
coalesce(nullif(gewicht,'nan')::numeric,0)::numeric  gewicht,
coalesce(nullif("stellplätze",'nan'),'0')::text stellplaetze
from {{source("prototype","transport_data")}}
where prozess not in ('19.03.2018','781148/781')
) q1