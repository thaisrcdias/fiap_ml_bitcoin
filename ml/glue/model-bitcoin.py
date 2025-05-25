import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pickle
import boto3
import pandas as pd
from datetime import datetime
import numpy as np
import awswrangler as wr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'Open', 'High', 'Low', 'Close', 'Volume', 'bucket_name','model_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client = boto3.client('s3')

client.download_file(Bucket=args['bucket_name'], Key='modelo/model/'+args['model_name'], Filename=args['model_name'])
modelo = pickle.load(open(args['model_name'], 'rb'))

client.download_file(Bucket=args['bucket_name'], Key='modelo/data/file.snappy.parquet', Filename='file.snappy.parquet')

# Carregar dados (ajuste o caminho do arquivo)
df_hist = pd.read_parquet('file.snappy.parquet')

json = [{"Date":datetime.now().strftime("%Y=%m-%d %H:%M:%S"),"Open":float(args['Open']),"High":float(args['High']),"Low":float(args['Low']),"Close":float(args['Close']),"Volume":float(args['Volume'])}]

def json_to_df(json):
    df_score = pd.DataFrame(json)
    return df_score
    
def generate_features(df_hist, df):
    df_resposta = df.copy()
    df_resposta.rename(columns={'Date':'Date_2'}, inplace=True)
    df_resposta = df_resposta[['Date_2']] 
    
    df = pd.concat([df_hist.reset_index(drop=True), df.reset_index(drop=True)])
    df.sort_values("Date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # features baseadas em janelas de tempo
    df['log_return_1d'] = np.log(df['Close'] / df['Close'].shift(1)) #essa
    df[f'rolling_std_30'] = df['Close'].rolling(30).std()
    df[f'rolling_mean_7'] = df['Close'].rolling(7).mean()
    df[f'rolling_std_7'] = df['Close'].rolling(7).std()
    df[f'zscore_7'] = (df['Close'] - df[f'rolling_mean_7']) / (df[f'rolling_std_7'] + 1e-9)

    # features baseadas em momento de mercado
    df['momentum_3d'] = df['Close'] - df['Close'].shift(3)

    # features baseadas em RSI (sugestão do chatGPT) em uma janela de 14 dias.
    #O RSI varia entre 0 e 100, e indica:
     #   RSI > 70 → ativo sobrecomprado (pode estar esticado, possível correção ou queda).
     #   RSI < 30 → ativo sobrevendido (pode estar barato, possível recuperação).
     #   RSI entre 30 e 70 → zona neutra.
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    df['rsi_14'] = 100 - (100 / (1 + rs))

    df = pd.merge(df_resposta, df, how='left', left_on='Date_2', right_on='Date')
    df.drop(['Date_2'], axis=1)

    return df
    
def selector(df):
    colunas = ['Volume', 'log_return_1d', 'zscore_7', 'rolling_std_30', 'momentum_3d', 'rsi_14']
    return df[colunas]
    
def model(df, modelo):
    df['score'] = modelo.predict_proba(df)[:, 1]
    df_score['data'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['valor_sobe'] = (df['score']>=0.5).astype(int)
    return df
    
df_score = json_to_df(json)
df_score = generate_features(df_hist, df_score)
df_score = selector(df_score)
df_score = model(df_score, modelo)

print(df_score.head(1).to_json(orient='records'))

wr.s3.to_parquet(df=df_score, path='s3://fiap-files-512988434617/modelo/results/', dataset=True, database='db_source_bitcoin', table='tbl_results_model', mode='append')

job.commit()