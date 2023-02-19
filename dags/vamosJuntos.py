from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import zipfile
import random
import pandas as pd
from pymongo import MongoClient
import boto3
import requests
from io import StringIO 
import io
from sqlalchemy import create_engine
from airflow.models import Variable
import json


# AWS credentials
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
aws_bucket_name = Variable.get('aws_bucket_name')
aws_region_name = Variable.get('aws_region_name')

#mongoDb credentials
mongodb_host = Variable.get('mongodb_host')
mongodb_user = Variable.get('mongodb_user')
mongodb_password = Variable.get('mongodb_password')

#MYSQL Credentials
mysql_user = Variable.get('mysql_user')
mysql_password = Variable.get('mysql_password')
mysql_host     = Variable.get('mysql_host')


default_args = {
    'owner': 'Tales Monteiro',
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 30, 18, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False
    #"retries": 1,
    #"retry_delay": timedelta(minutes=1),
}


dag = DAG(
    "vamosJuntos", 
    description="Projeto ELT para a empresa Vamos Juntos",
    default_args=default_args, 
    schedule_interval='@once'
)


def extractDataIbge():

    #extrai os dados do ibge
    url = 'https://servicodados.ibge.gov.br/api/v1/localidades/microrregioes'
    dataIbge = requests.get(url)
    dataIbge = dataIbge.json()

    # armazena os mesmos dados no nosso dataLake na camada raw
    iso_date = datetime.now().isoformat()
    key_file = f'raw/ibge/microrregioes-json-list-{iso_date[:-7]}.json'
    bucket = aws_bucket_name
    s3 = boto3.resource('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    obj = s3.Object(bucket, key_file)
    obj.put(Body=json.dumps(dataIbge))
    return key_file

extractDataIbge = PythonOperator(
    task_id='extractDataIbge',
    python_callable=extractDataIbge,
    dag=dag
)

def extractDataMongoDb():
    # extrai os dados do mongoDb
    client = MongoClient(f'mongodb+srv://{mongodb_user}:{mongodb_password}@{mongodb_host}/ibge?retryWrites=true&w=majority')
    db = client.ibge
    pnad_collec = db.pnadc20203
    dataMongoDb = list(pnad_collec.find({}, {'_id': 0}))

    # armazena os mesmos dados no nosso dataLake na camada raw
    iso_date = datetime.now().isoformat()
    key_file = f'raw/ibge/pnadc20203-{iso_date[:-7]}.json'
    bucket = aws_bucket_name
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    obj = s3.Object(bucket, key_file)
    obj.put(Body=json.dumps(dataMongoDb))
    
    return key_file


extractDataMongoDb = PythonOperator(
    task_id="extractDataMongoDb",
    python_callable=extractDataMongoDb,
    dag=dag
)


def processingMongodBData(**context):
    key_file = context['task_instance'].xcom_pull(task_ids='extractDataMongoDb')
    print(f'Teste => {key_file} ')
    


    #Resgata os dados do mongodb da camada raw
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    bucket = aws_bucket_name
    obj = s3.Object(bucket, key_file)

    #limpeza e tratamento de dados
    jsonsMongoDb = json.loads(obj.get()['Body'].read())
    jsonsMongoDbDf = pd.DataFrame(jsonsMongoDb)
    jsonsMongoDbDf = jsonsMongoDbDf.fillna('')
    jsonsMongoDbDf['renda'] = pd.to_numeric(jsonsMongoDbDf['renda'])

    # após os dados limpos e tratados, armazenar na camada silver
    csv_buffer = StringIO()
    iso_date = datetime.now().isoformat()
    key_file_silver = f'silver/ibge/pnadc20203-{iso_date[:-7]}.csv'
    jsonsMongoDbDf.to_csv(csv_buffer, encoding='utf-8', sep=";", index=False)
    s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3_resource.Object(bucket, key_file_silver).put(Body=csv_buffer.getvalue())

    return key_file_silver


processingMongodBData = PythonOperator(
    task_id="processingMongodBData",
    python_callable=processingMongodBData,
    provide_context=True,
    dag=dag
)


def insertDataInDw(**context):

    key_file_mongo_db = context['task_instance'].xcom_pull(task_ids='processingMongodBData')
    key_file_ibge = context['task_instance'].xcom_pull(task_ids='extractDataIbge')


    # pegar dados mongoDb da camada silver
    s3 = boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region_name)
    obj = s3.get_object(Bucket=aws_bucket_name, Key=key_file_mongo_db)
    pnadc20203 = pd.read_csv(io.BytesIO(obj['Body'].read()), sep=";")

    #pegar dados do ibge da camada raw
    objJson = s3.get_object(Bucket=aws_bucket_name, Key=key_file_ibge)
    jsonsIbge = json.loads(objJson['Body'].read())

    #Conversão dos dados do IBGE para o formato mais adequado para a empresa 
    microregioes = []
    for x in jsonsIbge:
        dicionario = {'id': x['id'], 'nome': x['nome'], 'mesorregiaoId': x['mesorregiao']['id']}
        microregioes.append(dicionario) 

    mesorregioes = []
    for x in jsonsIbge:
        dicionario = {'id': x['mesorregiao']['id'], 'nome': x['mesorregiao']['nome'], 'estadoId': x['mesorregiao']['UF']['id']}
        mesorregioes.append(dicionario)

    estados = []
    for x in jsonsIbge:
        dicionario = {'id': x['mesorregiao']['UF']['id'], 'nome': x['mesorregiao']['UF']['nome'], 'regiaoId': x['mesorregiao']['UF']['regiao']['id']}
        estados.append(dicionario)
    
    regioes = []
    for x in jsonsIbge:
        dicionario = {'id': x['mesorregiao']['UF']['regiao']['id'], 'nome': x['mesorregiao']['UF']['regiao']['nome']}
        regioes.append(dicionario)


    microregioesDf = pd.DataFrame(microregioes)
    mesorregioesDf = pd.DataFrame(mesorregioes)
    estadosDf = pd.DataFrame(estados)
    regioesDf = pd.DataFrame(regioes)

    mesorregioesDf = mesorregioesDf.drop_duplicates()
    estadosDf = estadosDf.drop_duplicates()
    regioesDf = regioesDf.drop_duplicates()
    
    #Conversão dos dados do MongoDB para o formato mais adequado para a empresa 
    pnadc20203Tratado = pnadc20203.merge(estadosDf, left_on='uf', right_on='nome')
    pnadc20203Tratado['idEstado'] = pnadc20203Tratado['id']
    pnadc20203Tratado = pnadc20203Tratado[(pnadc20203Tratado['sexo'] == 'Mulher') & (pnadc20203Tratado['idade'] >= 20 ) &  (pnadc20203Tratado['idade'] <= 40)]
    pnadc20203Tratado = pnadc20203Tratado.drop(['id', 'nome', 'regiaoId'], axis=1)  


    #inserindo os dados tratados e convertidos adequadamente no nosso datalake (camada gold)

    #def uploadCsv(dataframe, key_file):
        #csv_buffer = StringIO()
        #iso_date = datetime.now().isoformat()
       #key_file_gold = f'{key_file}-{iso_date[:-7]}.csv'
        #dataframe.to_csv(csv_buffer, encoding='utf-8', sep=";", index=False)
        #s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        #s3_resource.Object(aws_bucket_name, key_file_gold).put(Body=csv_buffer.getvalue())


    #uploadCsv(microregioesDf,'gold/ibge/microregioes')
    #uploadCsv(mesorregioesDf,'gold/ibge/mesorregioes')
    #uploadCsv(estadosDf,'gold/ibge/estados')
    #uploadCsv(regioesDf,'gold/ibge/regioes')
    #uploadCsv(pnadc20203Tratado,'gold/ibge/pnadc20203')

    #Inserindo no DW
    engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/vamosjuntos')

    pnadc20203Tratado.to_sql("pnadc20203", con=engine, index=False, if_exists='append')
    microregioesDf.to_sql("microregioes", con=engine, index=False, if_exists='append')
    mesorregioesDf.to_sql("mesorregioes", con=engine, index=False, if_exists='append')
    estadosDf.to_sql("estados", con=engine, index=False, if_exists='append')
    regioesDf.to_sql("regioes", con=engine, index=False, if_exists='append')


 
insertDataInDw = PythonOperator(
    task_id="insetDataInDw",
    python_callable=insertDataInDw,
    provide_context=True,
    dag=dag
)


[extractDataIbge,extractDataMongoDb]
processingMongodBData.set_upstream(extractDataMongoDb)
insertDataInDw.set_upstream([extractDataIbge,processingMongodBData])