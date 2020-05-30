from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, CreateTableOperator)
from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator
from dimension_SubDAG import dimension_SubDAG
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = "udacity-dend-warehouse"
s3_song_key = "song_data"
s3_log_key = "log_data"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'udacity',
    'depends_on_past' : True,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
    'catchup' : True
   }
root_dag_name = "udac_example_dag"
dag = DAG(root_dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 2
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create table
create_tables_in_redshift = CreateTableOperator(task_id='create_tables', conn_id='redshift', dag=dag)

# load data to tables
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    conn_id="redshift",
    aws_credential_id="aws_credentials", 
    table_name="staging_events", 
    s3_bucket=s3_bucket, 
    s3_key=s3_log_key, 
    file_format="JSON", 
    log_json_file=log_json_file,
    provide_context = True,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    conn_id="redshift",
    aws_credential_id="aws_credentials", 
    table_name="staging_songs", 
    s3_bucket=s3_bucket, 
    s3_key=s3_song_key, 
    file_format="JSON", 
    log_json_file=log_json_file,
    provide_context = True,
    dag=dag
)

# Create fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    conn_id = 'redshift',
    query = SqlQueries.songplay_table_insert, 
    dag=dag
)

# Dim tables:
# users table
user_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_user_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.user_table_insert, table_name="users")
load_user_dimension_table = SubDagOperator(subdag=user_subdag, task_id='Load_user_dim_table', dag=dag)
# songs table
song_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_song_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.song_table_insert, table_name="songs")
load_song_dimension_table = SubDagOperator(subdag=song_subdag, task_id='Load_song_dim_table', dag=dag)
# artists table
artist_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_artist_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.artist_table_insert, table_name="artists")
load_artist_dimension_table = SubDagOperator(subdag=artist_subdag, task_id='Load_artist_dim_table', dag=dag)
# time table
time_subdag = dimension_SubDAG(parent_dag=root_dag_name, task_id='Load_time_dim_table', conn_id="redshift", start_date=default_args['start_date'], query=SqlQueries.time_table_insert, table_name="time")
load_time_dimension_table = SubDagOperator(subdag=time_subdag, task_id='Load_time_dim_table', dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    conn_id="redshift",
    tables=["time", "users", "artists", "songplays", "songs"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# order
start_operator >> create_tables_in_redshift >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

