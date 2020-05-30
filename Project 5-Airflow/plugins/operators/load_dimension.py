from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, conn_id="", query="", table_name="", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.query = query
        self.table_name = table_name

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.conn_id)
        # delete original table
        redshift_hook.run(f"DELETE FROM {self.table_name}")
        # then create
        redshift_hook.run(self.query)
        self.log.info(f"Dim table {self.table_name} loaded")
