from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook 
from airflow.hooks.postgres_hook import PostgresHook 

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self, conn_id="", aws_credential_id="", table_name="", s3_bucket="", s3_key="", log_json_file="", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.conn_id = conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')
        self.copy_csv_query = "COPY '{}' FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' IGNOREHEADER 1 FORMAT AS CSV DELIMITER '{}';"
        self.copy_parquet_query = "COPY '{}' FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS PARQUET;"
        

    def execute(self, context):
        redshift_hook = PostgreHook(postgres_conn_id=self.conn_id)
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Loading files to table {self.table_name} from: {s3_path}")
            
        self.copy_csv_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, ',')
        self.copy_parquet_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key)
        
        # register a Postgre hook on redshift cluster
        
        # run query to load data
        redshift_hook.run(self.copy_csv_query)
        redshift_hook.run(self.copy_parquet_query)
        
        self.log.info(f"Table {self.table_name} staged.")





