CREATE TABLE dim_administratie (
administratie_key INTEGER NOT NULL,
a_tenant INTEGER NOT NULL,
a_naam VARCHAR(120) NOT NULL,
a_code VARCHAR(10),
a_alternatieve_code VARCHAR(10),
dwh_modified_date VARCHAR(20) NOT NULL,
a_source_modified VARCHAR(20),
a_source_id INTEGER,
a_source_adminnr VARCHAR(20) NOT NULL,
PRIMARY KEY (administratie_key)
);

CREATE TABLE dim_grootboek (
grootboek_key INTEGER NOT NULL,
gb_tenant INTEGER NOT NULL,
gb_naam VARCHAR(120) NOT NULL,
gb_rekeningnr INTEGER NOT NULL,
gb_verdichting_code_1 INTEGER,
gb_verdichting_naam_1 VARCHAR(120),
gb_verdichting_toonnaam_1 VARCHAR(130),
gb_verdichting_code_2 INTEGER,
gb_verdichting_naam_2 VARCHAR(120),
gb_verdichting_toonnaam_2 VARCHAR(130),
gb_verdichting_code_3 INTEGER,
gb_verdichting_naam_3 VARCHAR(120),
gb_verdichting_toonnaam_3 VARCHAR(130),
dwh_modified_date VARCHAR(20) NOT NULL,
gb_source_modified VARCHAR(20),
gb_source_id INTEGER,
gb_source_adminnr VARCHAR(20) NOT NULL,
PRIMARY KEY (grootboek_key)
);

CREATE TABLE dim_kostenplaats (
kostenplaats_key INTEGER NOT NULL,
kp_tenant INTEGER NOT NULL,
kp_naam VARCHAR(120) NOT NULL,
kp_code VARCHAR(10),
kp_alternatieve_code VARCHAR(10),
dwh_modified_date VARCHAR(20) NOT NULL,
kp_source_modified VARCHAR(20),
kp_source_id INTEGER,
kp_source_adminnr VARCHAR(20) NOT NULL,
PRIMARY KEY (kostenplaats_key)
);

CREATE TABLE organisatie (
id INTEGER NOT NULL,
version INTEGER,
sorteervolgorde INTEGER NOT NULL,
actief SMALLINT NOT NULL,
financieeladminnummer VARCHAR(10),
financieelcode VARCHAR(50),
hrmcode VARCHAR(10),
naam VARCHAR(200) NOT NULL,
organisatieniveau INTEGER NOT NULL,
parent INTEGER,
afnemer INTEGER NOT NULL,
PRIMARY KEY (id)
);

CREATE TABLE closure_organisatie (
organisatie_key INTEGER NOT NULL,
parent INTEGER NOT NULL
);

CREATE INDEX ClosureHashIndex ON closure_organisatie (parent);

CREATE TABLE month_names (
month INTEGER NOT NULL,
month_name VARCHAR(9) UNIQUE NOT NULL,
PRIMARY KEY (month)
);

CREATE TABLE dim_tijdtabel (
pk_date VARCHAR(20) NOT NULL,
financial_month INTEGER NOT NULL,
date_name VARCHAR(50),
year VARCHAR(20),
year_name VARCHAR(50),
year_number INTEGER,
quarter VARCHAR(20),
quarter_name VARCHAR(50),
month VARCHAR(20),
month_name VARCHAR(50),
week VARCHAR(20),
week_name VARCHAR(50),
day_of_year INTEGER,
day_of_year_name VARCHAR(50),
day_of_quarter INTEGER,
day_of_quarter_name VARCHAR(50),
day_of_month INTEGER,
day_of_month_name VARCHAR(50),
day_of_week INTEGER,
day_of_week_name VARCHAR(50),
week_of_year INTEGER,
week_of_year_name VARCHAR(50),
month_of_year INTEGER,
month_of_year_name VARCHAR(50),
month_of_quarter INTEGER,
month_of_quarter_name VARCHAR(50),
quarter_of_year INTEGER,
quarter_of_year_name VARCHAR(50),
quarter_of_half_year INTEGER,
quarter_of_half_year_name VARCHAR(50),
month_name_of_year VARCHAR(10),
school_year VARCHAR(20),
school_year_name VARCHAR(50),
year_month_index INTEGER,
year_month_day_index INTEGER,
month_of_year_index_name VARCHAR(50),
PRIMARY KEY (pk_date)
);

CREATE TABLE fact_exploitatie (
dwh_id INTEGER NOT NULL,
tenant_key INTEGER NOT NULL,
organisatie_key INTEGER NOT NULL,
administratie_key INTEGER NOT NULL,
kostenplaats_key INTEGER NOT NULL,
kostendrager_key INTEGER NOT NULL,
year_key INTEGER NOT NULL,
month_key INTEGER NOT NULL,
project_key INTEGER NOT NULL,
grootboek_key INTEGER NOT NULL,
m_realisatiebedrag FLOAT NOT NULL,
m_budgetbedrag FLOAT NOT NULL,
t_boekstuknummer VARCHAR(120),
t_datum VARCHAR(20) NOT NULL,
t_omschrijving VARCHAR(120),
t_factuurnummer VARCHAR(120),
dwh_modified_date VARCHAR(20) NOT NULL,
t_source_modified VARCHAR(20),
t_source_id INTEGER,
tenant_year_key INTEGER NOT NULL,
PRIMARY KEY (dwh_id)
);
PARTITION TABLE fact_exploitatie ON COLUMN tenant_year_key;

CREATE INDEX FactOrganisatieHashIdx ON fact_exploitatie (organisatie_key);

CREATE PROCEDURE FROM CLASS procedures.Set1;
CREATE PROCEDURE FROM CLASS procedures.Set2;
CREATE PROCEDURE FROM CLASS procedures.BulkLoad;

PARTITION PROCEDURE Set1 ON TABLE fact_exploitatie COLUMN tenant_year_key;
PARTITION PROCEDURE Set2 ON TABLE fact_exploitatie COLUMN tenant_year_key;