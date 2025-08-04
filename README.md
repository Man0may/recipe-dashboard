# AWS Recipe Dashboard (Tasty API)

This project is an end-to-end AWS data pipeline using MWAA (Airflow) to ingest recipes from the Tasty API (via RapidAPI), transform the data in Glue, query it in Athena, and visualize trending recipes in a QuickSight dashboard.

## Workflow
1. Airflow DAG runs weekly in MWAA
2. Lambda fetches recipe data (Tasty API) → stores raw JSON in S3
3. AWS Glue ETL job transforms raw data → saves processed Parquet to S3
4. Athena queries processed data via Glue Data Catalog
5. QuickSight dashboard visualizes recipe trends, filters vegetarian recipes

