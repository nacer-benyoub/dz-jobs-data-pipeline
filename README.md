## Overview
This is an ETL data pipeline that extracts job market data from two Algerian job platforms [Emploitic](emploitic.com) and [EmploiPartner](emploipartner.com) via API calls and transforms it to populate a dimensional model using a star schema, feeding a Metabase dashboad that contains various metrics and charts that track Algerian job market supply and demand.

## Architecture
The pipeline follows a medallion architecture with 3 layers:
- **Bronze layer**: raw and staging data.
- **Silver layer**: fact and dimension tables.
- **Gold layer**: pre-joined bridge and dimension tables as views and pre-aggregated data.

## Challenges
- Modeling many-to-many relashionships using bridge tables.
- Handling a string encoding issue (`é` -> `u00e9`, `ï` -> `u00ef`) caused by Mage when loading text[] columns into Postgres. which teached me about the `unistr` function in Postgres.
- Standardization of same columns coming from the two sources in different formats and languages.

## Future work
- Add more data sources.
- Use Spark, Iceberg and Minio to implement a lakehouse architecture and account for distributed compute and storage.
- Use Airflow (or Astronomer) instead of Mage.