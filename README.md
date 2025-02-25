# Music Streams Data Engineering Project 🎵🚀
This project focuses on building a scalable data pipeline for processing and analyzing music streaming data using AWS services. The goal is to efficiently ingest, transform, and store streaming data, enabling insightful analytics and reporting.

![Untitled Diagram drawio](https://github.com/user-attachments/assets/eac5b609-2a11-493b-83b9-c2cde9c99920)

## 🔧 Tech Stack & Tools
* AWS Glue – ETL processing and data transformation
* Amazon Redshift – Data warehousing for scalable analytics
* Amazon S3 – Storage for raw and processed data
* MWAA (Managed Workflows for Apache Airflow) – Orchestration of data pipelines

## 📌 Project Workflow
* Data Ingestion – Music streaming data is collected and stored in S3.
* ETL Processing – AWS Glue transforms raw data into structured formats.
* Data Loading – Processed data is stored in Redshift for querying.
* Pipeline Orchestration – MWAA (Airflow) schedules and monitors workflows.
* Data Analytics & Reporting – Run SQL queries on Redshift for insights.

## Airflow DAG Walkthrough: Data Validation and KPI Computation

### Overview
This DAG automates the following steps:

* Validate Datasets: Check if required columns exist in CSV files stored in AWS S3.
* Conditional Execution: If validation passes, trigger Spark jobs in AWS Glue.
* Wait for Job Completion: Poll for the status of Glue jobs.
* Write KPIs to Redshift: After successful execution, data is upserted into Amazon Redshift.
* Archive Processed Files: Move processed files to an archive folder in S3.

### Step-by-Step Walkthrough
#### 1. DAG Configuration
* The DAG is named data_validation_and_kpi_computation.
* The schedule interval is set to daily.
* The DAG starts from days_ago(1), meaning it runs immediately after deployment.

#### 2. Data Validation from AWS S3

**Task: validate_datasets**
* Reads songs.csv, users.csv, and all user_streams/*.csv files from S3.
* Checks if required columns exist in each dataset.
* If any dataset is missing columns, logs an error and fails the task.

**Task: check_validation**
* Uses a BranchPythonOperator to decide execution flow:
* If all datasets are valid, proceed to KPI computation.
* If validation fails, the DAG ends.

**Task: end_dag**
* If validation fails, the DAG ends execution here.

#### 3. Triggering AWS Glue Jobs

**Task: trigger_spark_genre_level_kpis_job_task**
* Starts a Glue Spark job named "genre_level_kpis" to compute music genre-related KPIs.

**Task: wait_for_spark_genre_level_kpis_job_completion_task**
* Polls Glue every 60 seconds until the "genre_level_kpis" job is complete.

**Task: trigger_spark_hourly_kpis_job_task**
* Starts another Glue Spark job named "hourly-kpis" to compute hourly user engagement KPIs.

**Task: wait_for_spark_hourly_kpis_job_completion_task**
* Waits for the "hourly-kpis" Glue job to complete.

#### 4. Writing Data to Redshift
**Task: write_genre_level_kpis_to_redshift**
* Reads genre-level KPI results from S3 (music_streams/output/genre_level_kpis/).
* Upserts the data into Amazon Redshift using a temporary table.

**Task: write_hourly_kpis_to_redshift**
* Reads hourly KPI results from S3 (music_streams/output/hourly-kpis/).
* Upserts the data into Amazon Redshift.

#### 5. Moving Processed Files to Archive
**Task: move_files**
* Moves processed user_streams/*.csv files to user-streams-archived/ in S3.
* Ensures that duplicate processing does not occur.

![Screenshot 2025-02-24 222516](https://github.com/user-attachments/assets/93b5f95d-9e1b-46bb-9304-0891f1ba02af)

### Output
#### S3 Bucket

![Screenshot 2025-02-24 222614](https://github.com/user-attachments/assets/7c75ae26-2cf6-4c92-a4db-8a62fa39b695)

![Screenshot 2025-02-24 222537](https://github.com/user-attachments/assets/abe3ae0c-83ea-413f-a9fb-e09771a97aa7)

#### Redshift

![Screenshot 2025-02-24 222746](https://github.com/user-attachments/assets/797f4bbb-54c7-4b9d-bd31-34d197492108)

![Screenshot 2025-02-24 222723](https://github.com/user-attachments/assets/fca758cf-005e-4a4a-8f06-c486440cca23)



