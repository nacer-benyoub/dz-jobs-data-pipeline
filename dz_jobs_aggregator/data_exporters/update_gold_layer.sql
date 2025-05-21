CREATE SCHEMA IF NOT EXISTS {{ env_var('POSTGRES_GOLD_SCHEMA') }};

CREATE OR REPLACE VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.vw_job_function AS
SELECT 
    br.job_id,
    br.function_id,
    df.function_description
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.br_job_function br
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.dim_function df ON br.function_id = df.function_id;

CREATE OR REPLACE VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.vw_job_location AS
SELECT 
    br.job_id,
    br.location_id,
    dl.city,
    dl.state,
    dl.region,
    dl.country
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.br_job_location br
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.dim_location dl ON br.location_id = dl.location_id;

CREATE OR REPLACE VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.vw_job_level AS
SELECT 
    br.job_id,
    br.job_level_id,
    jl.job_level_description
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.br_job_job_level br
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.dim_job_level jl ON br.job_level_id = jl.job_level_id;

CREATE OR REPLACE VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.vw_job_contract_type AS
SELECT 
    br.job_id,
    br.contract_type_id,
    ct.contract_type_description
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.br_job_contract_type br
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.dim_contract_type ct ON br.contract_type_id = ct.contract_type_id;

CREATE OR REPLACE VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.vw_job_education_level AS
SELECT 
    br.job_id,
    br.education_level_id,
    ed.education_level_description
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.br_job_education_level br
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.dim_education_level ed ON br.education_level_id = ed.education_level_id;

CREATE OR REPLACE VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.vw_job_experience_requirements AS
SELECT 
    br.job_id,
    br.experience_requirements_id,
    ex.experience_requirements_description
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.br_job_experience_requirements br
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.dim_experience_requirements ex ON br.experience_requirements_id = ex.experience_requirements_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ env_var('POSTGRES_GOLD_SCHEMA') }}.mv_job_performance_summary AS
SELECT
    p.job_id,
    SUM(p.nb_views) AS total_views,
    SUM(p.nb_applicants) AS total_applicants,
    MIN(p.date) AS first_active_day,
    MAX(p.date) AS last_active_day
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.fct_job_performance_daily p
GROUP BY p.job_id
WITH NO DATA;
REFRESH MATERIALIZED VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.mv_job_performance_summary;

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ env_var('POSTGRES_GOLD_SCHEMA') }}.mv_job_first_last_application_days AS
SELECT
    p.job_id,
    MIN(p.date) FILTER (WHERE p.nb_applicants > 0) - f.datetime_published::date AS days_to_first_applicant,
    MAX(p.date) FILTER (WHERE p.nb_applicants > 0) - f.datetime_published::date AS days_to_last_applicant
FROM {{ env_var('POSTGRES_SILVER_SCHEMA') }}.fct_job_performance_daily p
JOIN {{ env_var('POSTGRES_SILVER_SCHEMA') }}.fct_jobs f ON f.job_id = p.job_id
GROUP BY p.job_id, f.datetime_published
WITH NO DATA;
REFRESH MATERIALIZED VIEW {{ env_var('POSTGRES_GOLD_SCHEMA') }}.mv_job_first_last_application_days;
