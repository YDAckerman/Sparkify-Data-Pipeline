from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator,
                               PostgresOperator, RunOnceBranchOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Yoni Ackerman',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'catchup': True
}

dag = DAG('update_from_events_dag',
          default_args=default_args,
          description='Load and transform Event data in Redshift with Airflow',
          schedule_interval='@daily'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql"
)

# aws s3 ls s3://udacity-dend/log_data --recursive --human-readable --profile <profile>
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data/{}/{}/{}-{}-{}-events.json'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="staging_events",
    origin_columns=["userid", "firstname", "lastname",
                    "gender", "level"],
    destination_table="users",
    destination_columns=["userid", "first_name", "last_name",
                         "gender", "level"],
    where="page = 'NextSong'",
    conflict_column="userid",
    on_conflict_do="UPDATE SET level = EXCLUDED.level"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
