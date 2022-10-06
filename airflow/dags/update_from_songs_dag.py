from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator,
                               PostgresOperator, RunOnceBranchOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Yoni Ackerman',
    'start_date': datetime.now(),
}

dag = DAG('update_from_songs_dag',
          default_args=default_args,
          description='Load and transform Song data in Redshift with Airflow',
          schedule_interval='@daily'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    origin_table="staging_songs",
    origin_columns=["song_id", "title", "artist_id",
                    "year", "duration"],
    destination_table="songs",
    destination_columns=["songid", "title", "artistid",
                         "year", "duration"]
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    origin_table="staging_songs",
    origin_columns=["artist_id", "artist_name", "artist_location",
                    "artist_latitude", "artist_longitude"],
    destination_table="artists",
    destination_columns=["artistid", "name", "location",
                         "latitude", "longitude"]
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
