from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Yoni Ackerman',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 3),
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='ELT for Sparkify Data Warehouse',
          schedule_interval='@hourly',
          max_active_runs=1,
          catchup=True
          )

start_operator = DummyOperator(task_id='Begin_execution',
                               dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_task',
    dag=dag,
    table='staging_events',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data/{}/{}/{}-{}-{}-events.json',
    json='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_task',
    dag=dag,
    table='staging_songs',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    run_once=True
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    table='songplays',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_songs_dim_table',
    dag=dag,
    table='songs',
    run_once=True,
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    table='artists',
    run_once=True,
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    overwrite=True,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id='redshift',
    dag=dag,
    sql=SqlQueries.quality_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table

load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
