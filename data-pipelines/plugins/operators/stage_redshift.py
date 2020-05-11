from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    STAGE_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    format as JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 redshift_conn_id,
                 table,
                 s3_path,
                 region,
                 json_path,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run('''DELETE FROM {}'''.format(self.table))
        self.log.info(f"aws credentials: {credentials.access_key}, {credentials.secret_key}")
        redshift_hook.run(StageToRedshiftOperator.STAGE_SQL.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path))
