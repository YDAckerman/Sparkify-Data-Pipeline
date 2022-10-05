from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    """

    # from lesson 4
    #     staging_events_copy = ("""
    # COPY staging_events FROM '{}'
    # CREDENTIALS 'aws_iam_role={}'
    # json '{}'
    # region 'us-west-2';
    # """).format(config.get('S3', 'LOG_DATA'),
    #             config.get('DWH', 'ROLE_ARN'),
    #             config.get('S3', 'LOG_JSONPATH'))
    #
    # staging_songs_copy = ("""
    # COPY staging_songs FROM '{}'
    # CREDENTIALS 'aws_iam_role={}'
    # json 'auto'
    # region 'us-west-2';
    # """).format(config.get('S3', 'SONG_DATA'),
    #             config.get('DWH', 'ROLE_ARN'))

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json

    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting...')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from " +
                      f"Redshift table: {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)
