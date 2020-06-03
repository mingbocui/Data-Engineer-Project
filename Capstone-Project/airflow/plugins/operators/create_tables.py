from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    
    ui_color = '#89DD00'
    @apply_defaults
    def __init__(self, redshift_conn_id="", sql_query_path=None, *args, **kwargs):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        if sql_query_path is None:
            self.sql_query_path = "/home/workspace/airflow/create_tables.sql"
        else:
            self.sql_query_path = sql_query_path
        self.create_table_queries = open(self.sql_query_path, 'r').read()
    
    # contents of the context dict https://bcb.github.io/airflow/execute-context
    def execute(self, context):
        self.log.info("Ready for creating tables!")
        redshift_hook = PostgreHook(postgres_conn_id = self.redshift_conn_id)
        redshift_hook(self.create_table_queries)
        self.log.info("Tables are created successfully!")
        