from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Arul Prakash Pugazhendi',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udacity-data-pipelines',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='staging_events',
    s3_path="s3://udacity-dend/log_data",
    region="us-west-2",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='staging_songs',
    s3_path="s3://udacity-dend/song_data",
    region="us-west-2",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.user_table_insert,
    append_data=False,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.song_table_insert,
    append_data=False,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.artist_table_insert,
    append_data=False,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.time_table_insert,
    append_data=False,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table='songplays',
    checks={'table_not_empty': '''SELECT COUNT(*) FROM {}'''},
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_user_dimension_table
stage_events_to_redshift >> load_time_dimension_table

stage_songs_to_redshift >> load_song_dimension_table
stage_songs_to_redshift >> load_artist_dimension_table

load_user_dimension_table >> load_songplays_table
load_time_dimension_table >> load_songplays_table
load_song_dimension_table >> load_songplays_table
load_artist_dimension_table >> load_songplays_table

load_songplays_table >> run_quality_checks

run_quality_checks >> end_operator
