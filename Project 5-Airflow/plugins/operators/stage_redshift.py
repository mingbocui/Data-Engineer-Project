from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook # connect with AWS
from airflow.hooks.postgres_hook import PostgresHook # come from different sources with aws_hook

class StageToRedshiftOperator(BaseOperator):
    """
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, conn_id="", aws_credential_id="", table_name="", s3_bucket="", s3_key="", file_format="", log_json_file="", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.conn_id = conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')
        self.copy_query = "COPY '{}' FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS json '{}';"
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Loading stage files to table {self.table_name} from: {s3_path}")
        if self.log_json_file is None:
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.log_json_file)
        else:
            # To load from JSON data using the 'auto' argument, the JSON data must consist of a set of objects. The key names must match the column names, but in this case, order doesn't matter.
            self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        # register a Postgre hook on redshift cluster
        redshift_hook = PostgreHook(postgres_conn_id=self.conn_id)
        # run query to load data
        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.table_name} staged.")





