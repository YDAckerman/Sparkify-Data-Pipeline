from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 run_once=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.run_once = run_once

    def execute(self, context):

        self.log.info('StageToRedshiftOperator starting...')

        prev_success = context.get('prev_execution_date_success')
        self.log.info("Previous Completion Interval Start: " + str(prev_success))

        if self.run_once and prev_success:
            # if this task has been successful, do not run again
            self.log.info("Task Previously completed. Skipping.")

        else:

            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            self.log.info(f"Clearing data from table: {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

            self.log.info("Copying data from S3 to Redshift")

            execution_date = datetime.strptime(context['ds'], '%Y-%m-%d')
            rendered_key = self.s3_key.format(execution_date.year,
                                              execution_date.month,
                                              execution_date.year,
                                              execution_date.month,
                                              execution_date.strftime("%d"))
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = self.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json
            )
            redshift.run(formatted_sql)
