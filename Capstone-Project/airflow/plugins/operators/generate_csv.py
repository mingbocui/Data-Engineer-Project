from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook # connect with AWS
from airflow.hooks.postgres_hook import PostgresHook # come from different sources with aws_hook

import pandas as pd
import csv
import s3fs

class GenerateCsvOperator(BaseOperator):
    """
    Extract sas file and generate csv file, save in s3 bucket
    """
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self, aws_credential_id="", s3_bucket="", s3_sas_key="", *args, **kwargs):
        super(GenerateCsvOperator, self).__init__(*args, **kwargs)

        self.aws_credential_id = aws_credential_id
        self.s3_bucket = s3_bucket
        self.s3_sas_key = s3_sas_key
        
    def execute(self, context):
        """
        extract data from sas file to csv format
        """
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        # get access to the s3
        s3 = s3fs.S3FileSystem(anon=False, key=credentials.access_key, secret=credentials.secret_key)
        
        s3_sas_path = "s3://{}/{}".format(self.s3_bucket, self.s3_sas_key)
        s3_csv_path = "s3://{}".format(self.s3_bucket)
        self.log.info(f"Start to extract infomation from SAS file to csv format!")
        
        with s3.open(s3_sas_path, 'r') as f:
            f_content = f.read()
            f_content = f_content.replace('\t', '')

        def code_mapper(file, idx):
            f_content2 = f_content[f_content.index(idx):]
            f_content2 = f_content2[:f_content2.index(';')].split('\n')
            f_content2 = [i.replace("'", "") for i in f_content2]
            dic = [i.split('=') for i in f_content2[1:]]
            dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
            return dic
        self.log.info(f"Extracted sas files")
        i94cit_res = code_mapper(f_content, "i94cntyl")
        i94port = code_mapper(f_content, "i94prtl")
        i94mode = code_mapper(f_content, "i94model")
        i94addr = code_mapper(f_content, "i94addrl")
        i94visa = {'1':'Business', '2':'Pleasure', '3':'Student'}

        # addre
        csv_names = ['i94city', 'i94port', 'i94model', 'i94addr', 'i94visa']
        dict_list = [i94cit_res, i94port, i94mode, i94addr, i94visa]
        column_name = ['country', 'port', 'model', 'address', 'visa']
        
        for i, csv_name in enumerate(csv_names):
            csv_columns = ['code', column_name[i]]
            csv_file = s3_csv_path + f"/{csv_name}.csv"
            self.log.info(csv_file)
            dict_data = dict_list[i]
            try:
                # NOTE to use s3.open
                with s3.open(csv_file, 'w') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                    writer.writeheader()
                    for key in dict_data.keys():
                        data = {'code':key, column_name[i]:dict_data.get(key)}
                        writer.writerow(data)
            except IOError:
                print("I/O error")
        self.log.info(f"Done with writing data to S3 bucket!")

