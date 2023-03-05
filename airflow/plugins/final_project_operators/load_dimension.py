from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate_before_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_before_load:
            self.log.info(f"Truncating table {self.table}")
            truncate_sql = LoadDimensionOperator.truncate_sql.format(self.table)
            redshift_hook.run(truncate_sql)
        self.log.info(f"Loading dimension table {self.table}")
        formatted_sql = self.sql.format(**context)
        redshift_hook.run(formatted_sql)
        self.log.info(f"Loading dimension table {self.table} complete")
