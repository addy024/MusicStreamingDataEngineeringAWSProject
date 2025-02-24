from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import boto3
import logging
import time 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants for S3 bucket and file paths
BUCKET_NAME = 'music-streams-data-engineering-batch-project'
SONGS_FILE_PATH = 'music_streams/songs/songs.csv'
USERS_FILE_PATH = 'music_streams/users/users.csv'
STREAMS_PREFIX = 'music_streams/user_streams/'

REQUIRED_COLUMNS = {
    'songs': ['id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity',
              'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
              'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness',
              'valence', 'tempo', 'time_signature', 'track_genre'],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
}

def list_s3_files(prefix, bucket=BUCKET_NAME):
    """ List all files in S3 bucket that match the prefix """
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
        logging.info(f"Successfully listed files with prefix {prefix} from S3: {files}")
        return files
    except Exception as e:
        logging.error(f"Failed to list files with prefix {prefix} from S3: {str(e)}")
        raise Exception(f"Failed to list files with prefix {prefix} from S3: {str(e)}")

def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """ Helper function to read a CSV file from S3 """
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return pd.read_csv(obj['Body'])
    except Exception as e:
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")

def validate_datasets():
    validation_results = {}

    # Validate songs dataset
    try:
        songs_data = read_s3_csv(SONGS_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['songs']) - set(songs_data.columns)
        if not missing_columns:
            validation_results['songs'] = True
            logging.info("All required columns present in songs")
        else:
            validation_results['songs'] = False
            logging.warning(f"Missing columns in songs: {missing_columns}")
    except Exception as e:
        validation_results['songs'] = False
        logging.error(f"Failed to read or validate songs from S3: {e}")
        raise

    # Validate users dataset
    try:
        users_data = read_s3_csv(USERS_FILE_PATH)
        missing_columns = set(REQUIRED_COLUMNS['users']) - set(users_data.columns)
        if not missing_columns:
            validation_results['users'] = True
            logging.info("All required columns present in users")
        else:
            validation_results['users'] = False
            logging.warning(f"Missing columns in users: {missing_columns}")
    except Exception as e:
        validation_results['users'] = False
        logging.error(f"Failed to read or validate users from S3: {e}")
        raise

    # Validate streams datasets
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        for stream_file in stream_files:
            streams_data = read_s3_csv(stream_file)
            missing_columns = set(REQUIRED_COLUMNS['streams']) - set(streams_data.columns)
            if not missing_columns:
                validation_results['streams'] = True
                logging.info(f"All required columns present in {stream_file}")
            else:
                validation_results['streams'] = False
                logging.warning(f"Missing columns in {stream_file}: {missing_columns}")
                break
    except Exception as e:
        validation_results['streams'] = False
        logging.error(f"Failed to read or validate streams from S3: {e}")
        raise

    return validation_results

def branch_task(ti):
    validation_results = ti.xcom_pull(task_ids='validate_datasets')
    
    if all(validation_results.values()):
        return 'trigger_spark_genre_level_kpis_job_task'
    else:
        return 'end_dag'
    
def upsert_to_redshift(df, table_name, id_columns):
    redshift_hook = PostgresHook(postgres_conn_id="redshift_default")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        # Create insert query for the temporary table
        cols = ', '.join(list(df.columns))
        vals = ', '.join(['%s'] * len(df.columns))
        tmp_table_query = f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})"

        cursor.executemany(tmp_table_query, data_tuples)

        # Create the merge (upsert) query
        delete_condition = ' AND '.join([f'tmp_{table_name}.{col} = {table_name}.{col}' for col in id_columns])
        merge_query = f"""
        BEGIN;
        DELETE FROM reporting_schema.{table_name}
        USING reporting_schema.tmp_{table_name}
        WHERE {delete_condition};

        INSERT INTO reporting_schema.{table_name}
        SELECT * FROM reporting_schema.tmp_{table_name};

        TRUNCATE TABLE reporting_schema.tmp_{table_name};
        COMMIT;
        """

        cursor.execute(merge_query)
        conn.commit()
        logging.info(f"Data ingested and merged successfully into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to ingest and merge data into {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def write_to_redshift(path, table_name, id_columns):
    output_files = list_s3_files(f'music_streams/output/{path}/')
    # file_name = f's3a://{BUCKET_NAME}/music_streams/output/{path}/'
    for filename in output_files:
        df = read_s3_csv(filename, bucket=BUCKET_NAME)
        upsert_to_redshift(df, table_name, id_columns)


def wait_for_glue_job_completion(job_name, client, poll_interval=60):
    while True:
        response = client.get_job_runs(JobName=job_name, MaxResults=1)
        job_runs = response.get('JobRuns', [])
        
        if job_runs and job_runs[0]['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
            logging.info(f"Glue job {job_name} is still running. Waiting for it to finish...")
            time.sleep(poll_interval)
        else:
            logging.info(f"Glue job {job_name} has finished.")
            break

def trigger_glue_job(job_name, **kwargs):
    client = boto3.client('glue', region_name='ap-south-1')
    logging.info(f"Triggering Glue job: {job_name}")
    response = client.start_job_run(JobName=job_name)

def wait_for_spark_job_completion(job_name, **kwargs):
    glue_job_name = job_name
    client = boto3.client('glue', region_name='ap-south-1')
    wait_for_glue_job_completion(glue_job_name, client)


def move_files_to_archived(**kwargs):
    s3 = boto3.client('s3')
    bucket = 'music-streams-data-engineering-batch-project'
    source_prefix = 'music_streams/user-streams/'
    dest_prefix = 'music_streams/user-streams-archived/'

    response = s3.list_objects_v2(Bucket=bucket, Prefix=source_prefix)
    for obj in response.get('Contents', []):
        source_key = obj['Key']
        dest_key = source_key.replace(source_prefix, dest_prefix)

        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=dest_key)
        s3.delete_object(Bucket=bucket, Key=source_key)

with DAG('data_validation_and_kpi_computation', default_args=default_args, schedule_interval='@daily') as dag:
    validate_datasets = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task,
        provide_context=True
    )

    trigger_spark_genre_level_kpis_job_task = PythonOperator(
    task_id='trigger_spark_genre_level_kpis_job_task',
    python_callable=trigger_glue_job,
    op_args=['genre_level_kpis']
    )

    wait_for_spark_genre_level_kpis_job_completion_task = PythonOperator(
    task_id='wait_for_spark_genre_level_kpis_job_completion_task',
    python_callable=wait_for_spark_job_completion,
    op_args=['genre_level_kpis']
    )

    trigger_spark_hourly_kpis_job_task = PythonOperator(
    task_id='trigger_spark_hourly_kpis_job_task',
    python_callable=trigger_glue_job,
    op_args=['hourly-kpis']
    )

    wait_for_spark_hourly_kpis_job_completion_task = PythonOperator(
    task_id='wait_for_spark_hourly_kpis_job_completion_task',
    python_callable=wait_for_spark_job_completion,
    op_args=['hourly-kpis']
    )

    write_genre_level_kpis_to_redshift = PythonOperator(
        task_id = 'write_genre_level_kpis_to_redshift',
        python_callable = write_to_redshift,
        op_args=['genre_level_kpis', "genre_level_kpis", ['listen_date', 'track_genre']],
    )

    write_hourly_kpis_to_redshift = PythonOperator(
        task_id = 'write_hourly_kpis_to_redshift',
        python_callable = write_to_redshift,
        op_args=['hourly-kpis', 'hourly_kpis',['listen_date', 'listen_hour']],
    )

    move_files = PythonOperator(
    task_id='move_files',
    python_callable=move_files_to_archived
    )

    end_dag = DummyOperator(
        task_id='end_dag'
    )

    validate_datasets >> check_validation >> [trigger_spark_genre_level_kpis_job_task, end_dag]
    trigger_spark_genre_level_kpis_job_task >> wait_for_spark_genre_level_kpis_job_completion_task >>trigger_spark_hourly_kpis_job_task >> wait_for_spark_hourly_kpis_job_completion_task >> write_genre_level_kpis_to_redshift >> write_hourly_kpis_to_redshift >> move_files
