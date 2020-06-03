from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTableOperator, GenerateCsvOperator, CopyInsertTableOperator, DataQualityOperator) #StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, 

from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator
# from dimension_SubDAG import dimension_SubDAG
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = "mingbo-bucket"
s3_sas_key = "I94_SAS_Labels_Descriptions.SAS"
s3_csv_key = "sas_to_csv"

# depends_on_past (boolean) when set to True, keeps a task from getting triggered if the previous schedule for the task hasn’t succeeded.

default_args = {
    'owner': 'mingbocui',
    'depends_on_past' : True,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5),
    'start_date': datetime(2020, 6, 2),
    'catchup' : False
   }

root_dag_name = "capstone_dag"
dag = DAG(root_dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# extract data from sas and save data to csv format 
# redshift_conn_id = conn_id used only when load data to 
generate_csv_operator = GenerateCsvOperator(task_id="Extract_sas", dag=dag,	aws_credential_id="aws_credentials", s3_bucket=s3_bucket, s3_sas_key=s3_sas_key)
create_tables_operator = CreateTableOperator(task_id="Create_All_Tables", dag=dag, redshift_conn_id="redshift")

# using subDAG?
# immigrations us_cities_demographics airport i94visas i94port i94mode i94cit i94addr
# airport_codes_csv.csv
load_airport_operator = CopyInsertTableOperator(task_id="Load_airport_Tables", dag=dag, table_name="airport", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="airport_codes_csv.csv")
load_immigration_operator = CopyInsertTableOperator(task_id="Load_immigration_Tables", dag=dag, table_name="immigrations", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="immigration_data_sample.csv")
load_demographics_operator = CopyInsertTableOperator(task_id="Load_demographics_Tables", dag=dag, table_name="us_cities_demographics", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="us-cities-demographics.csv")
load_i94addr_operator = CopyInsertTableOperator(task_id="Load_i94addr_Tables", dag=dag, table_name="i94addr", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="i94addr.csv")
load_i94city_operator = CopyInsertTableOperator(task_id="Load_i94city_Tables", dag=dag, table_name="i94cit", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="i94city.csv")
load_i94model_operator = CopyInsertTableOperator(task_id="Load_i94model_Tables", dag=dag, table_name="i94mode", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="i94model.csv")
load_i94port_operator = CopyInsertTableOperator(task_id="Load_i94port_Tables", dag=dag, table_name="i94port", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="i94port.csv")
load_i94visa_operator = CopyInsertTableOperator(task_id="Load_i94visa_Tables", dag=dag, table_name="i94visas", aws_credential_id="aws_credentials", redshift_conn_id="redshift", s3_bucket=s3_bucket, s3_csv_key="i94visa.csv")

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    conn_id="redshift",
    tables=["airport", "immigrations", "us_cities_demographics", "i94addr", "i94cit", "i94mode", "i94port", "i94visas"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> generate_csv_operator >> create_tables_operator 

# create_tables_operator >> load_airport_operator >> end_operator
# create_tables_operator >> load_immigration_operator >> end_operator
# create_tables_operator >> load_demographics_operator >> end_operator
# create_tables_operator >> load_i94addr_operator >> end_operator
# create_tables_operator >> load_i94city_operator >> end_operator
# create_tables_operator >> load_i94model_operator >> end_operator
# create_tables_operator >> load_i94port_operator >> end_operator
# create_tables_operator >> load_i94visa_operator >> end_operator

create_tables_operator >> [load_airport_operator, load_immigration_operator, load_demographics_operator, load_i94addr_operator, load_i94city_operator, load_i94model_operator, load_i94port_operator, load_i94visa_operator] >> run_quality_checks >> end_operator







# # create table
# create_tables_in_redshift = CreateTableOperator(task_id='create_tables', conn_id='redshift', dag=dag)

# # load data to tables
# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     conn_id="redshift",
#     aws_credential_id="aws_credentials", 
#     table_name="staging_events", 
#     s3_bucket=s3_bucket, 
#     s3_key=s3_log_key, 
#     file_format="JSON", 
#     log_json_file=log_json_file,
#     provide_context = True,
#     dag=dag
# )

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     conn_id="redshift",
#     aws_credential_id="aws_credentials", 
#     table_name="staging_songs", 
#     s3_bucket=s3_bucket, 
#     s3_key=s3_song_key, 
#     file_format="JSON", 
#     log_json_file=log_json_file,
#     provide_context = True,
#     dag=dag
# )

# # Create fact table
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     conn_id = 'redshift',
#     query = SqlQueries.songplay_table_insert, 
#     dag=dag
# )

# # Dim tables:
# # users table
# user_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_user_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.user_table_insert, table_name="users")
# load_user_dimension_table = SubDagOperator(subdag=user_subdag, task_id='Load_user_dim_table', dag=dag)
# # songs table
# song_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_song_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.song_table_insert, table_name="songs")
# load_song_dimension_table = SubDagOperator(subdag=song_subdag, task_id='Load_song_dim_table', dag=dag)
# # artists table
# artist_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_artist_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.artist_table_insert, table_name="artists")
# load_artist_dimension_table = SubDagOperator(subdag=artist_subdag, task_id='Load_artist_dim_table', dag=dag)
# # time table
# time_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_time_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.time_table_insert, table_name="time")
# load_time_dimension_table = SubDagOperator(subdag=time_subdag, task_id='Load_time_dim_table', dag=dag)

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     conn_id="redshift",
#     tables=["time", "users", "artists", "songplays", "songs"],
#     dag=dag
# )

# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# # order
# start_operator >> create_tables_in_redshift >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

