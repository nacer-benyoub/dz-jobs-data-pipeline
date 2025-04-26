-- Docs: https://docs.mage.ai/guides/sql-blocks

INSERT INTO {{ env_var("POSTGRES_BRONZE_SCHEMA") }}.stg_emploitic
SELECT
    job_id,
    title,
    company,

    -- casting because if all values in the column are null, its type becomes text
    CAST(_location AS TEXT[]) AS location,
    CAST(job_level AS TEXT[]) AS job_level,
    CAST(positions AS INTEGER) AS positions,
    CAST(datetime_published AS TIMESTAMP) AS datetime_published,
    sector,
    CAST(_function AS TEXT[]) AS function,
    CAST(contract_type AS TEXT[]) AS contract_type,
    CAST(education_level AS TEXT[]) AS education_level,
    CAST(experience_years AS TEXT[]) AS experience_years,
    CAST(is_anonymous AS BOOLEAN) AS is_anonymous,
    work_mode,
    CAST(date_scraped AS TIMESTAMP) AS date_scraped,
    job_source

FROM {{ df_1 }}
ON CONFLICT(job_id)
DO UPDATE SET
    location = EXCLUDED.location,
    job_level = EXCLUDED.job_level,
    positions = EXCLUDED.positions,
    sector = EXCLUDED.sector,
    function = EXCLUDED.function,
    contract_type = EXCLUDED.contract_type,
    education_level = EXCLUDED.education_level,
    experience_years = EXCLUDED.experience_years,
    is_anonymous = EXCLUDED.is_anonymous,
    work_mode = EXCLUDED.work_mode,
    date_scraped = EXCLUDED.date_scraped,
    job_source = EXCLUDED.job_source
