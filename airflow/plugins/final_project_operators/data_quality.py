from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        # Create a PostgresHook to connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Iterate over each test case and execute the SQL query
        for test in self.tests:
            sql = test.get('sql')
            expected_result = test.get('expected_result')

            # Execute the SQL query and get the actual result
            actual_result = redshift_hook.get_records(sql)[0][0]

            # Check if the actual result matches the expected result
            if actual_result != expected_result:
                error_message = f"Data quality check failed: expected {expected_result}, but got {actual_result}"
                raise ValueError(error_message)

            self.log.info(f"Data quality check passed: expected {expected_result}, got {actual_result}")
