from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'vps',
#     'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2019, 8, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    schema="public",
    insertion_table=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="public.users",
    insertion_table=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="public.songs",
    insertion_table=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="public.artists",
    insertion_table=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="public.time",
    insertion_table=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    test_predicates= {
        'songplays_loaded' : {
            'predicate': "SELECT COUNT(*) != 0 FROM PUBLIC.SONGPLAYS",
            'result': True
        },
        'songs_loaded' : {
            'predicate': "SELECT COUNT(*) != 0 FROM PUBLIC.songs",
            'result': True
        },
        'artists_loaded' : {
            'predicate': "SELECT COUNT(*) != 0 FROM PUBLIC.artists",
            'result': True
        },
        'users_loaded' : {
            'predicate': "SELECT COUNT(*) != 0 FROM PUBLIC.users",
            'result': True
        },
        'time_loaded' : {
            'predicate': "SELECT COUNT(*) != 0 FROM PUBLIC.time",
            'result': True
        },
        'userid_not_null':{
            'predicate': "SELECT COUNT(*)=0 FROM PUBLIC.SONGPLAYS WHERE userid is null",
            'result': True
        }
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
