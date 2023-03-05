from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import os

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 json_format="auto",
                 date_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.json_format = json_format
        self.date_format = date_format

    def execute(self, context):
        self.log.info("Connecting to S3...")
        hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        execution_date = context['execution_date']
        self.log.info(f"Execution date is {execution_date}")
        s3_key_rendered = self.s3_key.format(**context)
        self.log.info(f"Rendering s3_key: {self.s3_key} to {s3_key_rendered}")
        rendered_s3_path = f"s3://{self.s3_bucket}/{s3_key_rendered}"
        self.log.info(f"Executing copy command for {rendered_s3_path} to {self.table}")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_query = f"""
        COPY {self.table}
        FROM '{rendered_s3_path}'
        ACCESS_KEY_ID '{hook.get_credentials().access_key}'
        SECRET_ACCESS_KEY '{hook.get_credentials().secret_key}'
        DELIMITER '{self.delimiter}'
        IGNOREHEADER {self.ignore_headers}
        """
        if self.json_format != "auto":
            copy_query += f" JSON '{self.json_format}'"
        if self.date_format != "auto":
            copy_query += f" TIMEFORMAT '{self.date_format}'"
        redshift_hook.run(copy_query)
        self.log.info(f"Copying {self.table} from {rendered_s3_path} complete")
