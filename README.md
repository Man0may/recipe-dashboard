# Recipe Dashboard
AWS Recipe Dashboard (Tasty API)
This project is an end-to-end AWS data pipeline using MWAA (Airflow) to ingest recipes from the Tasty API (via RapidAPI), transform the data in Glue, query it in Athena, and visualize trending recipes in a QuickSight dashboard.

## Workflow
Lambda fetches recipe data (Tasty API) → stores raw JSON in S3
AWS Glue ETL job transforms raw data from the S3 bucket → saves processed Parquet to a separate S3 bucket
Athena queries processed data via Glue Data Catalog
QuickSight dashboard visualizes recipe trends, filters recipes based on factors such as length

Visualizes
<img width="2795" height="1084" alt="Screenshot 2025-09-07 125424" src="https://github.com/user-attachments/assets/232fccb3-9f91-4f6d-8de8-6b0937c5ce5a" />

<img width="1299" height="854" alt="Screenshot 2025-09-07 125440" src="https://github.com/user-attachments/assets/499c7cc7-5fc8-41ce-87ed-4b7cb8290601" />

<img width="1551" height="1019" alt="Screenshot 2025-09-07 130317" src="https://github.com/user-attachments/assets/ef760a27-a572-4994-ba38-006add84fa0d" />
