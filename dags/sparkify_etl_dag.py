from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

# Importing custom operators we created
from plugins.operators.stage_redshift import StageToRedshiftOperator  # Defined in plugins/operators/stage_redshift.py
from plugins.operators.load_fact import LoadFactOperator  # Defined in plugins/operators/load_fact.py
from plugins.operators.load_dimension import LoadDimensionOperator  # Defined in plugins/operators/load_dimension.py
from plugins.operators.data_quality import DataQualityOperator  # Defined in plugins/operators/data_quality.py
from helpers.sql_queries import SqlQueries  # Defined in helpers/sql_queries.py

# Default arguments for the DAG
default_args = {
    'owner': 'kareem',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
with DAG('sparkify_etl_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='@hourly',
         max_active_runs=1
         ) as dag:

    # ✅ Task: Create tables in Redshift using raw SQL script (from sql/create_tables.sql)
    create_tables = PostgresOperator(
        task_id='Create_tables',
        dag=dag,
        postgres_conn_id='redshift',
        sql='sql/create_tables.sql'
    )

    # ✅ Task: Start DAG execution (Dummy task for DAG structuring)
    start_operator = DummyOperator(task_id='Begin_execution')

    # ✅ Task: Stage events data from S3 to Redshift (logs JSON) into 'staging_events' table
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

    # ✅ Task: Stage songs data from S3 to Redshift (songs metadata) into 'staging_songs' table
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

    # ✅ Task: Load songplays fact table from staging tables using custom SQL
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert,
        append_data=False
    )

    # ✅ Task: Load users dimension table from staging data
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        append_data=False
    )

    # ✅ Task: Load songs dimension table from staging data
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        append_data=False
    )

    # ✅ Task: Load artists dimension table from staging data
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        append_data=False
    )

    # ✅ Task: Load time dimension table from transformed songplays data
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        append_data=False
    )

    # ✅ Task: Run data quality checks on final tables (songplays, users, songs, artists, time)
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    # ✅ Task: End DAG execution (Dummy task for DAG structuring)
    end_operator = DummyOperator(task_id='Stop_execution')

    # ✅ Task dependencies
    create_tables >> start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator
