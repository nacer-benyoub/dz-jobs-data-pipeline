-- Docs: https://docs.mage.ai/guides/sql-blocks
-- Docs: https://docs.mage.ai/guides/sql-blocks
-- Create temp tables
DROP TABLE IF EXISTS temp_daily_listings_snapshot_emploi_partner;
CREATE TEMP TABLE temp_daily_listings_snapshot_emploi_partner AS
WITH unnested_coalesced AS (
    SELECT
        job_id,
        title,
        COALESCE(company, 'Inconnu') as company,
        COALESCE(function, 'Inconnu') as function,
        COALESCE(city, 'Inconnu') AS city,
        COALESCE(state, 'Inconnu') AS state,
        COALESCE(region, 'Inconnu') AS region,
        COALESCE(country, 'Inconnu') AS country,
        COALESCE(job_level, 'Inconnu') AS job_level,
        positions,
        datetime_published,
        expire_date,
        COALESCE(sector, 'Inconnu') AS sector,
        unnest(COALESCE(NULLIF(NULLIF(contract_type, '{}'), ARRAY[NULL]), ARRAY['Inconnu'])) AS contract_type,
        COALESCE(education_level, 'Inconnu') AS education_level,
        COALESCE(experience_years, 'Inconnu') AS experience_years,
        is_anonymous,
        COALESCE(work_mode, 'Inconnu') as work_mode,
        has_salary,
        min_salary,
        max_salary,
        COALESCE(job_source, 'Inconnu') as job_source,
        date_scraped
    FROM {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploi_partner
    WHERE DATE(date_scraped) = DATE('{{ execution_date }}')
)
SELECT
    job_id,
    title,
    company,
    function,
    city,
    state,
    region,
    country,
    job_level,
    positions,
    datetime_published,
    expire_date,
    sector,
    CASE WHEN position('\' in contract_type) = 0 THEN
        unistr(regexp_replace(contract_type, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(contract_type)
    END as contract_type,
    education_level,
    experience_years,
    is_anonymous,
    work_mode,
    has_salary,
    min_salary,
    max_salary,
    job_source,
    date_scraped
FROM unnested_coalesced
;

-- Upsert into dimension tables
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company(company_name)
SELECT DISTINCT company as company_name
FROM temp_daily_listings_snapshot_emploi_partner
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function(function_description)
SELECT DISTINCT function as function_description
FROM temp_daily_listings_snapshot_emploi_partner
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location(city, state, region, country)
SELECT
    city,
    state,
    region,
    country
FROM temp_daily_listings_snapshot_emploi_partner
GROUP BY 1, 2, 3, 4
ON CONFLICT(city, state, country)
DO UPDATE SET region = EXCLUDED.region
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level(job_level_description)
SELECT DISTINCT job_level as job_level_description
FROM temp_daily_listings_snapshot_emploi_partner
WHERE job_level IS NOT NULL
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector(sector_description)
SELECT DISTINCT sector as sector_description
FROM temp_daily_listings_snapshot_emploi_partner
WHERE sector IS NOT NULL
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type(contract_type_description)
SELECT DISTINCT contract_type as contract_type_description
FROM temp_daily_listings_snapshot_emploi_partner
WHERE contract_type IS NOT NULL
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level(education_level_description)
SELECT DISTINCT education_level as education_level_description
FROM temp_daily_listings_snapshot_emploi_partner
WHERE education_level IS NOT NULL
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements(experience_requirements_description)
SELECT DISTINCT experience_years as experience_requirements_description
FROM temp_daily_listings_snapshot_emploi_partner
WHERE experience_years IS NOT NULL
ON CONFLICT DO NOTHING
;

-- Upsert into fact table
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs (
    job_id,
    title,
    company_id,
    sector_id,
    m_positions,
    datetime_published,
    expire_date,
    is_anonymous,
    work_mode,
    has_salary,
    min_salary,
    max_salary,
    job_source,
    date_scraped
)
SELECT
    DISTINCT ON (t.job_id) t.job_id,
    t.title,
    c.company_id,
    s.sector_id,
    t.positions AS m_positions,
    t.datetime_published,
    t.expire_date,
    t.is_anonymous,
    t.work_mode,
    t.has_salary,
    t.min_salary,
    t.max_salary,
    t.job_source,
    t.date_scraped
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company c
    ON t.company = c.company_name
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector s
    ON t.sector = s.sector_description
ORDER BY t.job_id, t.expire_date ASC
ON CONFLICT(job_id)
DO UPDATE SET
    company_id = EXCLUDED.company_id,
    sector_id = EXCLUDED.sector_id,
    m_positions = EXCLUDED.m_positions,
    expire_date = EXCLUDED.expire_date,
    is_anonymous = EXCLUDED.is_anonymous,
    work_mode = EXCLUDED.work_mode,
    has_salary = EXCLUDED.has_salary,
    min_salary = EXCLUDED.min_salary,
    max_salary = EXCLUDED.max_salary,
    job_source = EXCLUDED.job_source,
    date_scraped = EXCLUDED.date_scraped
;

-- Populate bridge tables
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_location (job_id, location_id)
SELECT 
    t.job_id, 
    dl.location_id
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location dl
    ON t.city = dl.city
    AND t.state = dl.state
    AND t.country = dl.country
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_job_level (job_id, job_level_id)
SELECT 
    t.job_id, 
    jl.job_level_id
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level jl 
    ON t.job_level = jl.job_level_description
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_function (job_id, function_id)
SELECT 
    t.job_id, 
    f.function_id
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function f
    ON t.function = f.function_description
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_contract_type (job_id, contract_type_id)
SELECT 
    t.job_id, 
    ct.contract_type_id
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type ct 
    ON t.contract_type = ct.contract_type_description
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_education_level (job_id, education_level_id)
SELECT 
    t.job_id, 
    el.education_level_id
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level el 
    ON t.education_level = el.education_level_description
ON CONFLICT DO NOTHING
;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_experience_requirements (job_id, experience_requirements_id)
SELECT 
    t.job_id, 
    er.experience_requirements_id
FROM temp_daily_listings_snapshot_emploi_partner t
JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements er 
    ON t.experience_years = er.experience_requirements_description
ON CONFLICT DO NOTHING
;

-- Populate the daily metrics fact table
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_job_performance_daily
-- pre-joined location bridge and dimension tables
WITH br_dim AS (
    SELECT
        br.job_id,
        br.location_id,
        dim.city,
        dim.state,
        dim.region,
        dim.country
    FROM {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_location br
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location dim
    ON br.location_id = dim.location_id
),
-- get new daily metrics for all jobs pulled today
raw_daily_snapshot AS (
    SELECT
        stg.job_id,
        bd.location_id,
        stg.datetime_published,
        stg.nb_applicants,
        stg.nb_views,
        stg.date_scraped
    -- using staging table instead of temp table because of the different grain
    -- temp table has unnested array columns so it has duplicate nb_views and nb_applicants
    FROM {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploi_partner stg
    JOIN br_dim bd
    USING(job_id, city, state, region, country)
    WHERE DATE(stg.date_scraped) = DATE('{{ execution_date }}')
),
-- get totals for active jobs
-- active = job that has both old and new metric values
active_jobs_metric_totals AS (
    SELECT
        fct.job_id,
        fct.location_id,
        SUM(fct.nb_applicants) as nb_apps_total,
        SUM(fct.nb_views) as nb_views_total
    FROM {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_job_performance_daily fct
    -- join to select only active jobs (filter out new jobs)
    JOIN raw_daily_snapshot daily
    USING(job_id, location_id)
    WHERE DATE(fct.date) < DATE('{{ execution_date }}')
    GROUP BY fct.job_id, fct.location_id
),
-- combine new and active jobs for insertion
daily_snapshot AS (
    SELECT
        daily.job_id,
        daily.location_id,
        DATE('{{ execution_date }}') as date,
        COALESCE(daily.nb_applicants - active.nb_apps_total, daily.nb_applicants) as nb_applicants,
        COALESCE(
            daily.nb_views - active.nb_views_total,
            daily.nb_views
        ) as nb_views,
        DATE('{{ execution_date }}') - DATE(daily.datetime_published) as days_since_published
    
    FROM raw_daily_snapshot daily
    LEFT JOIN active_jobs_metric_totals active
    USING(job_id, location_id)
)
SELECT
    job_id,
    location_id,
    date,
    nb_applicants,
    nb_views,
    days_since_published
FROM daily_snapshot
ON CONFLICT(job_id, location_id, date)
DO UPDATE SET
    nb_applicants = EXCLUDED.nb_applicants,
    nb_views = EXCLUDED.nb_views
;