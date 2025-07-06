-- Docs: https://docs.mage.ai/guides/sql-blocks
-- Create temp table
CREATE TEMP TABLE temp_daily_snapshot_emploitic AS
WITH unnested AS (
    SELECT
        job_id,
        title,
        COALESCE(company, 'Inconnu') AS company,
        COALESCE(sector, 'Inconnu') AS sector,
        unnest(COALESCE(NULLIF(location, ARRAY[NULL]), ARRAY['Inconnu'])) AS location,
        unnest(COALESCE(NULLIF(job_level, ARRAY[NULL]), ARRAY['Inconnu'])) AS job_level,
        positions,
        datetime_published,
        unnest(COALESCE(NULLIF(function, ARRAY[NULL]), ARRAY['Inconnu'])) AS function,
        unnest(COALESCE(NULLIF(contract_type, ARRAY[NULL]), ARRAY['Inconnu'])) AS contract_type,
        unnest(COALESCE(NULLIF(education_level, ARRAY[NULL]), ARRAY['Inconnu'])) AS education_level,
        unnest(COALESCE(NULLIF(experience_years, ARRAY[NULL]), ARRAY['Inconnu'])) AS experience_years,
        is_anonymous,
        COALESCE(work_mode, 'Inconnu') AS work_mode,
        COALESCE(job_source, 'Inconnu') AS job_source,
        date_scraped
    FROM {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploitic
    WHERE DATE(date_scraped) = DATE('{{ execution_date }}')
),
fixed_encoding AS (
    SELECT
        *,
        CASE WHEN position('\' in location) = 0 THEN
            unistr(regexp_replace(location, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(location)
        END AS location_fixed_encoding,

        CASE WHEN position('\' in job_level) = 0 THEN
            unistr(regexp_replace(job_level, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(job_level)
        END AS job_level_fixed_encoding,

        CASE WHEN position('\' in function) = 0 THEN
            unistr(regexp_replace(function, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(function)
        END AS function_fixed_encoding,

        CASE WHEN position('\' in contract_type) = 0 THEN
            unistr(regexp_replace(contract_type, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(contract_type)
        END AS contract_type_fixed_encoding,

        CASE WHEN position('\' in education_level) = 0 THEN
            unistr(regexp_replace(education_level, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(education_level)
        END AS education_level_fixed_encoding,

        CASE WHEN position('\' in experience_years) = 0 THEN
            unistr(regexp_replace(experience_years, 'u(\d+)', '\\u\1', 'g'))
        ELSE unistr(experience_years)
        END AS experience_years_fixed_encoding
    FROM unnested
),
split_locations AS (
    SELECT
        *,
        regexp_split_to_array(location_fixed_encoding, '\s*,\s*') AS location_array
    FROM fixed_encoding
)
SELECT
    job_id,
    title,
    company,
    sector,
    job_level_fixed_encoding AS job_level,
    function_fixed_encoding AS function,
    contract_type_fixed_encoding AS contract_type,
    education_level_fixed_encoding AS education_level,
    experience_years_fixed_encoding AS experience_years,
    positions,
    datetime_published,
    is_anonymous,
    work_mode,
    job_source,
    date_scraped,
    location_fixed_encoding AS location,
    CASE WHEN location_array IS NOT NULL THEN
        COALESCE(location_array[cardinality(location_array) - 3], 'Inconnu')
    END AS city,
    CASE WHEN location_array IS NOT NULL THEN
        COALESCE(location_array[cardinality(location_array) - 1], 'Inconnu')
    END AS state,
    CASE WHEN location_array IS NOT NULL THEN
        COALESCE(location_array[cardinality(location_array)], 'Inconnu')
    END AS country
FROM split_locations
;

-- Dimension tables
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company(company_name)
SELECT DISTINCT
    company AS company_name
FROM
    temp_daily_snapshot_emploitic
WHERE
    company IS NOT NULL
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector(sector_description)
SELECT DISTINCT
    sector AS sector_description
FROM
    temp_daily_snapshot_emploitic
WHERE
    sector IS NOT NULL
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location(city, state, country)
SELECT
    city,
    state,
    country
FROM
    temp_daily_snapshot_emploitic
WHERE city IS NOT NULL
    AND state IS NOT NULL
    AND country IS NOT NULL
GROUP BY
    city,
    state,
    country
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level(job_level_description)
SELECT DISTINCT
    job_level AS job_level_description
FROM
    temp_daily_snapshot_emploitic
WHERE
    job_level IS NOT NULL
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function(function_description)
SELECT DISTINCT
    function AS function_description
FROM
    temp_daily_snapshot_emploitic
WHERE
    function IS NOT NULL
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type(contract_type_description)
SELECT DISTINCT
    contract_type AS contract_type_description
FROM
    temp_daily_snapshot_emploitic
WHERE
    contract_type IS NOT NULL
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level(education_level_description)
SELECT DISTINCT
    education_level AS education_level_description
FROM
    temp_daily_snapshot_emploitic
WHERE
    education_level IS NOT NULL
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements(experience_requirements_description)
SELECT DISTINCT
    experience_years AS experience_requirements_description
FROM
    temp_daily_snapshot_emploitic
WHERE
    experience_years IS NOT NULL
ON CONFLICT
    DO NOTHING;

-- Fact table
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.fct_jobs (job_id, title, company_id, sector_id, m_positions, datetime_published, is_anonymous, work_mode, has_salary, job_source, date_scraped)
SELECT DISTINCT
    t.job_id,
    t.title,
    c.company_id,
    s.sector_id,
    t.positions AS m_positions,
    t.datetime_published,
    t.is_anonymous,
    t.work_mode,
    FALSE AS has_salary,
    t.job_source,
    t.date_scraped
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_company c
	ON t.company = c.company_name
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_sector s
	ON t.sector = s.sector_description
ON CONFLICT (job_id)
    DO UPDATE SET
        company_id = EXCLUDED.company_id,
        sector_id = EXCLUDED.sector_id,
        m_positions = EXCLUDED.m_positions,
        is_anonymous = EXCLUDED.is_anonymous,
        work_mode = EXCLUDED.work_mode,
        has_salary = EXCLUDED.has_salary,
        job_source = EXCLUDED.job_source,
        date_scraped = EXCLUDED.date_scraped;

-- Bridge tables
INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_location (job_id, location_id)
SELECT
    t.job_id,
    dl.location_id
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_location dl
	ON t.city = dl.city
        AND t.state = dl.state
        AND t.country = dl.country
ON CONFLICT
        DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_job_level (job_id, job_level_id)
SELECT
    t.job_id,
    jl.job_level_id
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_job_level jl
	ON t.job_level = jl.job_level_description
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_function (job_id, function_id)
SELECT
    t.job_id,
    f.function_id
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_function f
	ON t.function = f.function_description
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_contract_type (job_id, contract_type_id)
SELECT
    t.job_id,
    ct.contract_type_id
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_contract_type ct
	ON t.contract_type = ct.contract_type_description
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_education_level (job_id, education_level_id)
SELECT
    t.job_id,
    el.education_level_id
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_education_level el
	ON t.education_level = el.education_level_description
ON CONFLICT
    DO NOTHING;

INSERT INTO {{ env_var("POSTGRES_SILVER_SCHEMA") }}.br_job_experience_requirements (job_id, experience_requirements_id)
SELECT
    t.job_id,
    er.experience_requirements_id
FROM
    temp_daily_snapshot_emploitic t
    JOIN {{ env_var("POSTGRES_SILVER_SCHEMA") }}.dim_experience_requirements er
	ON t.experience_years = er.experience_requirements_description
ON CONFLICT
    DO NOTHING;

