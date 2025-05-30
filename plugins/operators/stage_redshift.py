from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)  # Allows dynamic values via templating (e.g. {{ ds }})

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="auto",
                 region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        # Store all parameters as instance variables
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region

    def execute(self, context):
        # Get AWS credentials from Airflow connection
        s3_hook = S3Hook(self.aws_credentials_id)
        credentials = s3_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear the destination table before loading new data
        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Format S3 path using context
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Build and execute the COPY command
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON '{self.copy_json_option}';
        """

        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        redshift.run(copy_sql)
