import sys
sys.path.append('/home/ed/airflow/dags/news_analytics/scripts')
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from upload_to_s3 import upload_to_s3
from process_article_data import process_article_data
import configparser


config = configparser.ConfigParser()
config.read("/home/ed/.postgres/credentials")
pg_password = config["default"]["eddy_password"]


with DAG(
    dag_id="news_analytics_etl",
    start_date=datetime(year=2025, month=2, day=20, hour =0, minute=0),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True
) as dag:
    
    upload_to_s3_task = PythonOperator(
      dag=dag,
      task_id="upload_to_s3",
      python_callable=upload_to_s3  
    )

    truncate_staging_task = SQLExecuteQueryOperator(
        task_id="truncate_staging_table",
        conn_id="pg_news_analytics",
        sql="""
            TRUNCATE public.staging_entity_counts
            """
    )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_article_data,
        op_kwargs={
            "file_name": "{{ ti.xcom_pull(task_ids='upload_to_s3')[0] }}",
            "file_date": "{{ ti.xcom_pull(task_ids='upload_to_s3')[1] }}"
        }
    )

    insert_entities_task = SQLExecuteQueryOperator(
        task_id="insert_entities",
        conn_id="pg_news_analytics",
        sql="sql/insert_entities_from_staging.sql"
    )


    upload_to_s3_task >> truncate_staging_task >> process_data_task >> insert_entities_task
