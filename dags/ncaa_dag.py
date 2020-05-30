from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, StageToRedshiftCSVOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, MarchMadnessQueries

default_args = {
    'owner': 'Arul Prakash Pugazhendi',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udacity-capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

mseasons = StageToRedshiftCSVOperator(
    task_id='Seasons',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MSeasons',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MSeasons.csv",
    region="us-east-1",
    append=False,
)

mteams = StageToRedshiftCSVOperator(
    task_id='Teams',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MTeams',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MTeams.csv",
    region="us-east-1",
    append=False,
)

mncaatourneydetailedresults = StageToRedshiftCSVOperator(
    task_id='Tourney_Results',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MNCAATourneyDetailedResults',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MNCAATourneyDetailedResults.csv",
    region="us-east-1",
    append=False,
)

mgamecities = StageToRedshiftCSVOperator(
    task_id='Game_Cities',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MGameCities',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MGameCities.csv",
    region="us-east-1",
    append=False,
)

mregularseasondetailedresults = StageToRedshiftCSVOperator(
    task_id='Regular_Season_Results',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MRegularSeasonDetailedResults',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MRegularSeasonDetailedResults.csv",
    region="us-east-1",
    append=False,
)

mteamcoaches = StageToRedshiftCSVOperator(
    task_id='Team_Coaches',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MTeamCoaches',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MTeamCoaches.csv",
    region="us-east-1",
    append=False,
)

mncaatourneyseeds = StageToRedshiftCSVOperator(
    task_id='Tourney_Seeds',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='MNCAATourneySeeds',
    s3_path="s3://ncaa-2020/2020DataFiles/2020-Mens-Data/MDataFiles_Stage1/MNCAATourneySeeds.csv",
    region="us-east-1",
    append=False,
)

results_summary_win_tourney = LoadFactOperator(
    task_id='ResultsSummary_WinningTeams_Tourney',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='ResultsSummary',
    sql=MarchMadnessQueries.win_tourney,
    append_data=False
)

results_summary_win_regular = LoadFactOperator(
    task_id='ResultsSummary_WinningTeams_Regular',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='ResultsSummary',
    sql=MarchMadnessQueries.win_regular,
    append_data=True
)

results_summary_loss_tourney = LoadFactOperator(
    task_id='ResultsSummary_LosingTeams_Tourney',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='ResultsSummary',
    sql=MarchMadnessQueries.loss_tourney,
    append_data=True
)

results_summary_loss_regular = LoadFactOperator(
    task_id='ResultsSummary_LosingTeams_Regular',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    table='ResultsSummary',
    sql=MarchMadnessQueries.loss_regular,
    append_data=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table='public.resultssummary',
    checks={'table_not_empty': '''SELECT COUNT(*) FROM {}'''},
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

stage1_operator = DummyOperator(task_id='stage_one', dag=dag)
stage2_operator = DummyOperator(task_id='stage_two', dag=dag)

start_operator >> mseasons
start_operator >> mteams

mseasons >> stage1_operator
mteams >> stage1_operator

stage1_operator >> mgamecities
stage1_operator >> mncaatourneydetailedresults
stage1_operator >> mregularseasondetailedresults
stage1_operator >> mteamcoaches
stage1_operator >> mncaatourneyseeds

mgamecities >> stage2_operator
mncaatourneydetailedresults >> stage2_operator
mregularseasondetailedresults >> stage2_operator
mteamcoaches >> stage2_operator
mncaatourneyseeds >> stage2_operator

stage2_operator >> results_summary_win_tourney
stage2_operator >> results_summary_win_regular
stage2_operator >> results_summary_loss_tourney
stage2_operator >> results_summary_loss_regular

results_summary_win_tourney >> run_quality_checks
results_summary_win_regular >> run_quality_checks
results_summary_loss_tourney >> run_quality_checks
results_summary_loss_regular >> run_quality_checks

run_quality_checks >> end_operator