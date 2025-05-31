from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

# ✅ Default arguments for the DAG
default_args = {
    'owner': 'kareem',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# ✅ DAG definition
with DAG('sparkify_etl_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@hourly',
         max_active_runs=1
         ) as dag:

    # ✅ Task: Create tables in Redshift
    create_tables = PostgresOperator(
        task_id='Create_tables',
        dag=dag,
        postgres_conn_id='redshift',
        sql='sql/create_tables.sql'
    )

    # ✅ Start task
    start_operator = DummyOperator(task_id='Begin_execution')

    # ✅ Stage events data
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',  # Use connection ID
        table='staging_events',
        s3_bucket='sparkify-data-lake',        # Replace with your actual bucket
        s3_key='log_data',
        copy_json_option='log_json_path.json',  # Replace with actual path if needed
        region='us-west-2'
    )

    # ✅ Stage songs data
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='sparkify-data-lake',
        s3_key='song_data',
        copy_json_option='auto',
        region='us-west-2'
    )

    # ✅ Load fact table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert,
        append_data=False
    )

    # ✅ Load dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        append_data=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        append_data=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        append_data=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        append_data=False
    )

    # ✅ Data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    # ✅ End task
    end_operator = DummyOperator(task_id='Stop_execution')

    # ✅ Task dependencies
    create_tables >> start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator
