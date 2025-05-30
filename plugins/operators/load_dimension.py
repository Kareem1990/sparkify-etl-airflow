from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_data = append_data

    def execute(self, context):
        self.log.info(f"Connecting to Redshift: {self.redshift_conn_id}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info(f"Clearing data from dimension table: {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Inserting data into dimension table: {self.table}")
        formatted_sql = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(formatted_sql)
        self.log.info(f"Data successfully loaded into {self.table}")
