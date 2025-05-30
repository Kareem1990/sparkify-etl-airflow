from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

# Importing custom operators we created
from plugins.operators.stage_redshift import StageToRedshiftOperator  # Defined in plugins/operators/stage_redshift.py
from plugins.operators.load_fact import LoadFactOperator  # Defined in plugins/operators/load_fact.py
from plugins.operators.load_dimension import LoadDimensionOperator  # Defined in plugins/operators/load_dimension.py
from plugins.operators.data_quality import DataQualityOperator  # Defined in plugins/operators/data_quality.py
from helpers.sql_queries import SqlQueries  # Defined in helpers/sql_queries.py

# Default arguments for the DAG (sparkify_etl_dag.py)
default_args = {
    'owner': 'kareem',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition (sparkify_etl_dag.py)
with DAG('sparkify_etl_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@hourly',
         max_active_runs=1
         ) as dag:

    # Dummy start task (sparkify_etl_dag.py)
    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage events data from S3 to Redshift staging table (uses StageToRedshiftOperator in stage_redshift.py)
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id=Variable.get("aws_conn_id"),
        table='staging_events',
        s3_bucket=Variable.get("s3_bucket"),
        s3_key=Variable.get("s3_prefix") + '/log_data',
        copy_json_option=Variable.get("s3_prefix") + '/log_json_path.json',
        region='us-west-2'
    )

    # Stage songs data from S3 to Redshift staging table (uses StageToRedshiftOperator in stage_redshift.py)
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id=Variable.get("aws_conn_id"),
        table='staging_songs',
        s3_bucket=Variable.get("s3_bucket"),
        s3_key=Variable.get("s3_prefix") + '/song_data',
        copy_json_option='auto',
        region='us-west-2'
    )

    # Load songplays fact table from staging tables (uses LoadFactOperator in load_fact.py)
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert,
        append_data=False
    )

    # Load users dimension table (uses LoadDimensionOperator in load_dimension.py)
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        append_data=False
    )

    # Load songs dimension table (uses LoadDimensionOperator in load_dimension.py)
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        append_data=False
    )

    # Load artists dimension table (uses LoadDimensionOperator in load_dimension.py)
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        append_data=False
    )

    # Load time dimension table (uses LoadDimensionOperator in load_dimension.py)
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        append_data=False
    )

    # Run data quality checks on all final tables (uses DataQualityOperator in data_quality.py)
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    # Dummy end task (sparkify_etl_dag.py)
    end_operator = DummyOperator(task_id='Stop_execution')

    # Define task dependencies (sparkify_etl_dag.py)
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator
