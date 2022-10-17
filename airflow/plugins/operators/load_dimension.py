from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql="",
                 overwrite=False,
                 run_once=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.overwrite = overwrite
        self.run_once = run_once

    def execute(self, context):

        # create redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        prev_success = context.get('prev_execution_date_success')
        self.log.info("Previous Completion Interval Start: " + str(prev_success))

        if self.run_once and prev_success:
            # if this task has been successful, do not run again
            self.log.info("Task set to run once. Skipping.")
        else:
            if self.overwrite:
                self.log.info("Table is set to be overwritten. " +
                              f"Clearing data from table: {self.table}")
                redshift.run(f"DELETE FROM {self.table}")

            self.log.info(f'Loading table {self.table}')
            redshift.run(self.sql)
