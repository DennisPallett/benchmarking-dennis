DROP TABLE IF EXISTS dim_administratie CASCADE;
CREATE TABLE dim_administratie (
	administratie_key integer primary key not null, 
	a_tenant integer not null, 
	a_naam character varying(120) not null, 
	a_code character varying(10), 
	a_alternatieve_code character varying(10), 
	dwh_modified_date timestamp without time zone not null, 
	a_source_modified timestamp without time zone, 
	a_source_id integer, 
	a_source_adminnr character varying(20) not null
);

DROP TABLE IF EXISTS dim_grootboek CASCADE;
CREATE TABLE dim_grootboek (
	grootboek_key integer not null, 
	gb_tenant integer not null, 
	gb_naam character varying(120) not null, 
	gb_rekeningnr integer not null, 
	gb_verdichting_code_1 integer, 
	gb_verdichting_naam_1 character varying(120), 
	gb_verdichting_toonnaam_1 character varying(130), 
	gb_verdichting_code_2 integer, 
	gb_verdichting_naam_2 character varying(120), 
	gb_verdichting_toonnaam_2 character varying(130), 
	gb_verdichting_code_3 integer, 
	gb_verdichting_naam_3 character varying(120), 
	gb_verdichting_toonnaam_3 character varying(130), 
	dwh_modified_date timestamp without time zone not null, 
	gb_source_modified timestamp without time zone, 
	gb_source_id integer, 
	gb_source_adminnr character varying(20) not null
)
PARTITION BY LIST (gb_tenant)
(
 PARTITION dummy VALUES ('0')
);

CREATE UNIQUE INDEX gb_idx ON dim_grootboek (grootboek_key);

DROP TABLE IF EXISTS dim_kostenplaats CASCADE;
CREATE TABLE dim_kostenplaats (
	kostenplaats_key integer primary key not null, 
	kp_tenant integer not null, 
	kp_naam character varying(120) not null, 
	kp_code character varying(10), 
	kp_alternatieve_code character varying(10), 
	dwh_modified_date timestamp without time zone not null, 
	kp_source_modified timestamp without time zone, 
	kp_source_id integer, 
	kp_source_adminnr character varying(20) not null
);

DROP TABLE IF EXISTS organisatie CASCADE;
CREATE TABLE organisatie (
	id integer primary key not null, 
	version integer, 
	sorteervolgorde integer not null, 
	actief smallint not null, 
	financieeladminnummer character varying(10), 
	financieelcode character varying(50), 
	hrmcode character varying(10), 
	naam character varying(200) not null, 
	organisatieniveau integer not null, 
	parent integer, 
	afnemer integer not null
);

DROP TABLE IF EXISTS closure_organisatie CASCADE;
CREATE TABLE closure_organisatie (
	organisatie_key integer not null, 
	parent integer not null
);

DROP TABLE IF EXISTS month_names CASCADE;
CREATE TABLE month_names (
	month integer primary key not null, 
	month_name character varying(9) not null
);

DROP TABLE IF EXISTS dim_tijdtabel CASCADE;
CREATE TABLE dim_tijdtabel (
	pk_date timestamp without time zone primary key not null, 
	financial_month integer not null, 
	date_name character varying(50), 
	year timestamp without time zone, 
	year_name character varying(50), 
	year_number integer, 
	quarter timestamp without time zone, 
	quarter_name character varying(50), 
	month timestamp without time zone, 
	month_name character varying(50), 
	week timestamp without time zone, 
	week_name character varying(50), 
	day_of_year integer, 
	day_of_year_name character varying(50), 
	day_of_quarter integer, 
	day_of_quarter_name character varying(50), 
	day_of_month integer, 
	day_of_month_name character varying(50), 
	day_of_week integer, 
	day_of_week_name character varying(50), 
	week_of_year integer, 
	week_of_year_name character varying(50), 
	month_of_year integer, 
	month_of_year_name character varying(50), 
	month_of_quarter integer, 
	month_of_quarter_name character varying(50), 
	quarter_of_year integer, 
	quarter_of_year_name character varying(50), 
	quarter_of_half_year integer, 
	quarter_of_half_year_name character varying(50), 
	month_name_of_year character varying(10), 
	school_year timestamp without time zone, 
	school_year_name character varying(50), 
	year_month_index integer, 
	year_month_day_index integer, 
	month_of_year_index_name character varying(50)
);

DROP TABLE IF EXISTS fact_exploitatie CASCADE;
CREATE TABLE fact_exploitatie (
	dwh_id integer not null, 
	tenant_key integer not null, 
	organisatie_key integer references organisatie(id) not null, 
	administratie_key integer references dim_administratie(administratie_key) not null, 
	kostenplaats_key integer references dim_kostenplaats(kostenplaats_key) not null, 
	kostendrager_key integer not null, 
	year_key integer not null, 
	month_key integer references month_names(month) not null, 
	project_key integer not null, 
	grootboek_key integer references dim_grootboek(grootboek_key) not null, 
	m_realisatiebedrag double precision not null, 
	m_budgetbedrag double precision not null, 
	t_boekstuknummer character varying(120), 
	t_datum timestamp without time zone not null, 
	t_omschrijving character varying(120), 
	t_factuurnummer character varying(120), 
	dwh_modified_date timestamp without time zone not null, 
	t_source_modified timestamp without time zone, 
	t_source_id integer, 
	tenant_year_key integer
) WITH (appendonly=true, orientation=column)
PARTITION BY LIST (tenant_year_key)
(
 PARTITION dummy VALUES ('0')
);

CREATE INDEX month_idx ON fact_exploitatie (month_key);