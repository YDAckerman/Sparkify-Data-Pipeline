from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql=[""],
                 expected="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.expected = expected
        if expected == "":
            expected = [(0, "gt")] * len(sql)
        if len(expected) != len(sql):
            raise ValueError("expected and sql must be the same length")

    def execute(self, context):

        self.log.info('Running Data Quality Checks')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i in range(len(self.sql)):

            records = redshift_hook.get_records(self.sql[i])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed: " +
                                 "no results returned")

            num_records = records[0][0]
            on_error_string = f"Data quality check {i} failed: "
            if self.expected[i][1] == "eq":
                if not num_records == self.expected[i]:
                    raise ValueError(on_error_string +
                                     "unexpected value returned")

            elif self.expected[i][1] == "gt":
                if not num_records > self.expected[i]:
                    raise ValueError(on_error_string +
                                     "unexpected value returned")

            elif self.expected[i][1] == "lt":
                if not num_records < self.expected[i]:
                    raise ValueError(on_error_string +
                                     "unexpected value returned")

        pass
