from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgreCustomOperator)
from helpers import SqlQueries
from airflow.models import Variable

### Configuring the DAG

# In the DAG, add `default parameters` according to these guidelines

# - The DAG does not have dependencies on past runs
# - On failure, the task are retried 3 times
# - Retries happen every 5 minutes
# - Catchup is turned off
# - Do not email on retry

default_args = {
    'owner': 'hieptt4',
    'start_date': datetime(2023, 9, 16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Create a DAG instance
dag = DAG(
    'main_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily',
)

# Define the start operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


sql_path = Variable.get("sql_path")

with open(sql_path, 'r') as f:
    sql_script = f.read()

create_tables = PostgreCustomOperator(
    dag=dag,
    task_id="Create_tables",
    postgres_conn_id="redshift",
    sql=sql_script
)


# Define the task to stage events data to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    format_path="s3://udacity-dend/log_json_path.json"
)

# Define the task to stage songs data to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    format_path="auto"
)

# Define the task to load data into the songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    query=SqlQueries.songplay_table_insert
)

# Define the task to load data into the user dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    is_append=False,
    query=SqlQueries.user_table_insert
)

# Define the task to load data into the song dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    is_append=False,
    query=SqlQueries.song_table_insert
)

# Define the task to load data into the artist dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    is_append=False,
    query=SqlQueries.artist_table_insert
)

# Define the task to load data into the time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    is_append=False,
    query=SqlQueries.time_table_insert
)

# Define the task to run data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names=[ "songplays", "songs", "artists",  "time", "users"]
)


# Define the end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set up the task dependencies
start_operator  >>  create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]  >> load_songplays_table  >> [ load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table] >> run_quality_checks >> end_operator
