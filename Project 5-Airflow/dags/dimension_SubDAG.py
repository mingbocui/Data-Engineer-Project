from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries

def dimension_SubDAG(parent_dag, task_id, conn_id, query, table_name, *args, **kwargs):
    # parent child relationship
    dag = DAG(f"{parent_dag}.{task_id}", **kwargs)
    # dimension table
    # `task_id` and `dag` no need to be included in params list when define
    dimension_table = LoadDimensionOperator(task_id=task_id, conn_id=conn_id, query=query, table_name=table_name, dag=dag)
    
    return dag
    
    