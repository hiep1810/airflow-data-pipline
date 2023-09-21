from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table_name in self.table_names:
            # Run your data quality checks for each table
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = redshift_hook.get_first(query)
            count = result[0]

            if count == 0:
                raise ValueError(f"Data quality check failed. Table {table_name} has no records.")

            print(f"Data quality checks passed for table {table_name}.")