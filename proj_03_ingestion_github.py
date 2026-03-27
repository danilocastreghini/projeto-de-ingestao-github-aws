from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Marcos_Senior_DE',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'PROJ_03_GitHub_Pipeline_Full',
    default_args=default_args,
    description='Pipeline Completo: Lambda (Ingestão) -> Glue (Transformação)',
    schedule=None, # Rodada manual para teste
    catchup=False,
    tags=['aws', 'lambda', 'glue', 'pyspark'],
) as dag:

    # 1. Task da Lambda (Camada Bronze)
    task_github_extraction = LambdaInvokeFunctionOperator(
        task_id='trigger_lambda_github',
        function_name='extrator-api-github-marcos', 
        aws_conn_id='aws_default',
        log_type='Tail',
    )

    # 2. Task do Glue (Camada Silver)
    # Importante: O nome do job_name deve ser exatamente o que você criou no console da AWS
    # 2. Task do Glue (Camada Silver)
    task_glue_transform = GlueJobOperator(
        task_id='trigger_glue_transformation',
        job_name='github-silver-transformer-marcos', 
        iam_role_name='LabGlueRole-Marcos', # <--- Nome que você pegou no print!
        aws_conn_id='aws_default',
        region_name='us-east-2', # <--- Região correta para Ohio
        wait_for_completion=True, 
    )

    # Definindo a orquestração: A seta (>>) cria a dependência
    task_github_extraction >> task_glue_transform