from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 is_append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table            = table
        self.query            = query
        self.is_append        = is_append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.is_append:
            self.log.info(f"Clearing data from Redshift dimension table: {self.table}")
            redshift.run("DELETE FROM {}".format(self.table))
            
        self.log.info(f"Loading Redshift dimension table: {self.table}")

        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.query
        )
        
        redshift.run(formatted_sql)