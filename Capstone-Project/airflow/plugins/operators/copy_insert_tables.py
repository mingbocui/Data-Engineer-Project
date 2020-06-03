from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyInsertTableOperator(BaseOperator):
    
    ui_color = '#90DD00'
    @apply_defaults
    def __init__(self, aws_credential_id="", table_name="", redshift_conn_id="", s3_bucket="", s3_csv_key="", *args, **kwargs):
        super(CopyInsertTableOperator, self).__init__(*args, **kwargs)
        self.aws_credential_id = aws_credential_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_csv_key = s3_csv_key
        self.table_name = table_name
        self.copy_csv_query = "COPY '{}' FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' IGNOREHEADER 1 FORMAT AS CSV DELIMITER ',';"
        
    def execute(self, context):
        redshift_hook = PostgreHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_csv_key)
        self.log.info(f"Loading files to table {self.table_name} from: {s3_path}")
            
        self.copy_csv_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, ',')
        
        # run query to load data
        redshift_hook.run(self.copy_csv_query)
        
        self.log.info(f"Table {self.table_name} has loaded data.")
        