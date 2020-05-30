from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, conn_id="", query="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.query = query

    def execute(self, context):
        
        redshift_hook = PostgreHook(postgres_conn_id=self.conn_id)
        redshift_hook.run(self.query)
        self.log.info('Fact table loaded.')
