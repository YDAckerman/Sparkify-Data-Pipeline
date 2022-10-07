from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql=[""],
                 expected=[0],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.expected = expected
        if len(sql) != len(expected):
            raise ValueError("sql and expected should be the same length")

    def execute(self, context):
        self.log.info('Running Data Quality Checks')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in range(len(self.sql)):

            records = redshift_hook.get_records(self.sql[i])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed: " +
                                 "no results returned")

            num_records = records[0][0]
            if num_records != self.expected[i]:
                raise ValueError("Data quality check failed: " +
                                 "unexpected value returned")

        pass
