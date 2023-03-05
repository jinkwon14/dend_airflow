from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


# Set connections and variables
# os.system("airflow connections add aws_credentials --conn-uri 'aws://AKIARONRSPPFS6YR67B7:rTrSpzsZyIf61oxLH0qsAA%2BqZlp4iStUR807vdp6@'")
# os.system("airflow connections add redshift --conn-uri 'redshift://admin:fkdlTm90%21@default.238975189030.us-east-1.redshift-serverless.amazonaws.com:5439/dev'")
# os.system("airflow variables set s3_bucket jin-udacity")
# os.system("airflow variables set s3_prefix data-pipelines")


# Configure the DAG
default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,    
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    start_date=datetime(2023, 3, 4),
    schedule_interval=timedelta(days=1),
)

def final_project():
    # Define tasks
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='{{ var.value.s3_bucket }}',
        s3_key='{{ var.value.s3_prefix }}/log_data',
        json_format='s3://{{ var.value.s3_bucket }}/{{ var.value.s3_prefix }}/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='{{ var.value.s3_bucket }}',
        s3_key='{{ var.value.s3_prefix }}/song_data',
        json_format='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        postgres_conn_id='redshift',
        sql=final_project_sql_statements.songplay_table_insert,
        append_only=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        postgres_conn_id='redshift',
        sql=final_project_sql_statements.song_table_insert,
        append_only=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        postgres_conn_id='redshift',
        sql=final_project_sql_statements.user_table_insert,
        append_only=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.artist_table_insert,
        append_only=False        
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.time_table_insert,
        append_only=False        
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        postgres_conn_id="redshift",
        sql=final_project_sql_statements.songplay_table_insert,
        append_only=False           
    )

    end_operator = DummyOperator(task_id='End_execution')

    # define task dependency 
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

final_project_dag = final_project()
