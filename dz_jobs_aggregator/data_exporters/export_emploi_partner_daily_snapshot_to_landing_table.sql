-- Docs: https://docs.mage.ai/guides/sql-blocks
INSERT INTO {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploi_partner
SELECT
    job_id,
    title,
    company,

    -- casting because if all values in the column are null, its type becomes text
    CAST(positions AS INTEGER) AS positions,
    CAST(datetime_published AS TIMESTAMP) AS datetime_published,
    CAST(expire_date AS TIMESTAMP) AS expire_date,
    city,
    _state as state,
    region,
    country,
    job_level,
    sector,
    _function as function,
    CAST(contract_type AS TEXT[]) AS contract_type,
    education_level,
    experience_years,
    CAST(is_anonymous AS BOOLEAN) AS is_anonymous,
    work_mode,
    CAST(has_salary AS BOOLEAN) AS has_salary,
    CAST(min_salary AS INTEGER) AS min_salary,
    CAST(max_salary AS INTEGER) AS max_salary,
    CAST(nb_applicants AS INTEGER) AS nb_applicants,
    CAST(nb_views AS INTEGER) AS nb_views,
    CAST(date_scraped AS DATE) AS date_scraped,
    job_source

FROM {{ df_1 }}
ON CONFLICT(job_id, country, region, state, city, date_scraped)
DO UPDATE SET
    positions = EXCLUDED.positions,
    expire_date = EXCLUDED.expire_date,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    region = EXCLUDED.region,
    country = EXCLUDED.country,
    job_level = EXCLUDED.job_level,
    sector = EXCLUDED.sector,
    function = EXCLUDED.function,
    contract_type = EXCLUDED.contract_type,
    education_level = EXCLUDED.education_level,
    experience_years = EXCLUDED.experience_years,
    work_mode = EXCLUDED.work_mode,
    is_anonymous = EXCLUDED.is_anonymous,
    has_salary = EXCLUDED.has_salary,
    min_salary = EXCLUDED.min_salary,
    max_salary = EXCLUDED.max_salary,
    nb_applicants = EXCLUDED.nb_applicants,
    nb_views = EXCLUDED.nb_views,
    job_source = EXCLUDED.job_source
