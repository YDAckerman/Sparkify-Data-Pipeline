from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql="",
                 overwrite=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.overwrite = overwrite
        self.table = table

    def execute(self, context):
        # create redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.overwrite:
            self.log.info("Table is set to be overwritten. " +
                          f"Clearing data from table: {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info('Loading Fact Table')
        redshift.run(self.sql)
