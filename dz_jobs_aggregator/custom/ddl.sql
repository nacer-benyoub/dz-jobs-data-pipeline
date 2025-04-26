-- Docs: https://docs.mage.ai/guides/sql-blocks

----------------- Bronze schema DDL ------------------------
CREATE SCHEMA IF NOT EXISTS {{ env_var("POSTGRES_BRONZE_SCHEMA") }};

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploitic (
    job_id text NOT NULL,
    title text NOT NULL,
    company text,
    location text[],
    job_level text[],
    positions integer,
    datetime_published timestamp without time zone NOT NULL,
    sector text,
    function text[],
    contract_type text[],
    education_level text[],
    experience_years text[],
    is_anonymous boolean,
    work_mode text,
    date_scraped date,
    job_source text DEFAULT 'Inconnu'::text,
    PRIMARY KEY(job_id)
);

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploi_partner (
    job_id text NOT NULL,
    title text NOT NULL,
    company text,
    positions integer,
    datetime_published timestamp without time zone NOT NULL,
    expire_date timestamp without time zone,
    city text,
    state text,
    region text,
    country text,
    job_level text,
    sector text,
    function text,
    contract_type text[],
    education_level text,
    experience_years text,
    is_anonymous boolean,
    work_mode text,
    has_salary boolean,
    min_salary integer,
    max_salary integer,
    nb_applicants integer,
    nb_views integer,
    date_scraped date,
    job_source text DEFAULT 'Inconnu'::text,
    PRIMARY KEY(job_id)
);

----------------- Silver schema DDL ------------------------ 
CREATE SCHEMA IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }};

-- create dimension tables
CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company  (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
    -- TODO: add company data
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location  (
    location_id SERIAL PRIMARY KEY,
    city TEXT NOT NULL DEFAULT 'Inconnu'::TEXT,
    state TEXT NOT NULL DEFAULT 'Inconnu'::TEXT,
    region TEXT NOT NULL DEFAULT 'Inconnu'::TEXT,
    country TEXT NOT NULL DEFAULT 'Inconnu'::TEXT,
    UNIQUE (city, state, country)
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location(location_id)
SELECT -1 AS location_id
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level  (
    job_level_id SERIAL PRIMARY KEY,
    job_level_description TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector  (
    sector_id SERIAL PRIMARY KEY,
    sector_description TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function  (
    function_id SERIAL PRIMARY KEY,
    function_description TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type  (
    contract_type_id SERIAL PRIMARY KEY,
    contract_type_description TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level  (
    education_level_id SERIAL PRIMARY KEY,
    education_level_description TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements  (
    experience_requirements_id SERIAL PRIMARY KEY,
    experience_requirements_description TEXT UNIQUE NOT NULL DEFAULT 'Inconnu'::TEXT
);
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements
SELECT -1, 'Inconnu'
ON CONFLICT DO NOTHING
;

-- create the fact table
CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs (
    job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company(company_id) ON DELETE CASCADE,
    sector_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector(sector_id) ON DELETE CASCADE,
    m_positions INTEGER,
    datetime_published TIMESTAMP NOT NULL,
    expire_date TIMESTAMP WITHOUT TIME ZONE,
    is_anonymous BOOLEAN,
    work_mode TEXT,
    has_salary BOOLEAN,
    min_salary INTEGER,
    max_salary INTEGER,
    job_source text DEFAULT 'Inconnu'::TEXT,
    date_scraped DATE,
    PRIMARY KEY (job_id)
);

-- create the fct_job_performance_daily table
CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_job_performance_daily (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id),
    date DATE,
    nb_applicants INTEGER,
    nb_views INTEGER,
    PRIMARY KEY (job_id, date)
);

-- create bridge tables
CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_location (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id) ON DELETE CASCADE,
    location_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location(location_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, location_id)
);

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_job_level (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id) ON DELETE CASCADE,
    job_level_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level(job_level_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, job_level_id)
);

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_function (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id) ON DELETE CASCADE,
    function_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function(function_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, function_id)
);

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_contract_type (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id) ON DELETE CASCADE,
    contract_type_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type(contract_type_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, contract_type_id)
);

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_education_level (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id) ON DELETE CASCADE,
    education_level_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level(education_level_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, education_level_id)
);

CREATE TABLE IF NOT EXISTS {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_experience_requirements (
    job_id TEXT REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs(job_id) ON DELETE CASCADE,
    experience_requirements_id INTEGER REFERENCES {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements(experience_requirements_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, experience_requirements_id)
);
