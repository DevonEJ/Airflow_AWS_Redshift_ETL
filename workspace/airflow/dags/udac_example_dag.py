from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

sql_queries = SqlQueries()

start_date = datetime(2019, 1, 12)
default_args = {
    'owner': 'udacity',
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

dag = DAG('udacity_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_s3_bucket=Variable.get('aws_s3_events_bucket'),
    destination_staging_table='staging_events',
    format_as_json_path='s3://udacity-dend/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_s3_bucket=Variable.get('aws_s3_songs_bucket'),
    destination_staging_table='staging_songs',
    format_as_json_path='auto',
    dag=dag
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
    output_table='songplays',
    sql_query=sql_queries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
    output_table='users',
    insert_mode='truncate',
    sql_query=sql_queries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
    output_table='songs',
    insert_mode='truncate',
    sql_query=sql_queries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
    output_table='artists',
    insert_mode='truncate',
    sql_query=sql_queries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    aws_iam_role="arn:aws:iam::718240196138:role/myRedshiftRole",
    output_table='time',
    insert_mode='truncate',
    sql_query=sql_queries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table_list=['time', 'users', 'songs', 'songplays', 'artists'],
    primary_key_columns_list=['start_time', 'userid', 'songid', 'playid', 'artistid'],
    results_list=[0, 0, 0, 0, 0],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Outline dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> [load_song_dimension_table, load_time_dimension_table, load_user_dimension_table, load_artist_dimension_table]
stage_songs_to_redshift >> [load_song_dimension_table, load_time_dimension_table, load_user_dimension_table, load_artist_dimension_table]
[load_song_dimension_table, load_time_dimension_table, load_user_dimension_table, load_artist_dimension_table]  >> run_quality_checks
run_quality_checks >> end_operator