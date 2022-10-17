from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    NO_RESULTS = 'no results returned.'
    UNEXP_VALUE = 'unexpected value returned.'
    UNEXP_OP = 'unexpected operation requested.'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql=[""],
                 expected=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

        if len(expected) == 1 and expected[0] == "":
            expected = [(0, 'gt')] * len(sql)

        self.expected = expected

        if len(expected) != len(sql):
            raise ValueError("expected and sql must be the same length")

    def execute(self, context):

        self.log.info('Running Data Quality Checks')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if len(self.sql) == 1 and self.sql[0] == "":
            self.log.info('No Data Quality Checks to Pass')
            pass

        for i in range(len(self.sql)):

            records = redshift_hook.get_records(self.sql[i])
            on_error_string = f"Data quality check {i} failed: "
            on_pass_string = f'Check {i} Passed'
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(on_error_string + self.NO_RESULTS)

            if self.expected[i][1] not in ['gt', 'eq', 'lt']:
                raise ValueError(self.UNEXP_OP)

            num_records = records[0][0]
            if self.expected[i][1] == 'eq':
                if not num_records == self.expected[i][0]:
                    raise ValueError(on_error_string + self.UNEXP_VALUE)
                self.log.info(on_pass_string)
            elif self.expected[i][1] == 'gt':
                if not num_records > self.expected[i][0]:
                    raise ValueError(on_error_string + self.UNEXP_VALUE)
                self.log.info(on_pass_string)
            elif self.expected[i][1] == 'lt':
                if not num_records < self.expected[i][0]:
                    raise ValueError(on_error_string + self.UNEXP_VALUE)
                self.log.info(on_pass_string)
