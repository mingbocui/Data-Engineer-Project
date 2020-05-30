from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.check_operator import CheckOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, conn_id="", tables=[] *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        # redshift_hook = PostgresHook(postgres_conn_id = self.conn_id)
        
        for table in self.tables:
            if CheckOperator(sql="SELECT * FROM {}".format{table}, conn_id=self.conn_id):
                self.log.info(f"Data quality check for table {table} not pass!")
            else:
                self.log.info(f"Data quality check for table {table} pass!")