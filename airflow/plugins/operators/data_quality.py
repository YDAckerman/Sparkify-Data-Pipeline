from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def check_records(self):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records("SELECT COUNT(*) " +
                                            f"FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. " +
                             f"{self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. " +
                             f"{self.table} contained 0 rows")
        pass

    def check_nulls(self):
    
        
    def execute(self, context):
        self.log.info(f'Running DataQualityOperator on Table {self.table}')
