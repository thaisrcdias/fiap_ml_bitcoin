import yfinance as yf
import pandas as pd
from io import StringIO
import logging
import boto3
import os
import uuid

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s:%(funcName)s:%(message)s")

bucket_name_raw = "raw-209112358514"


s3 = boto3.client("s3")

def lambda_handler(event, context):
    try:
        # Criar um nome único para o arquivo com base no timestamp e no UUID
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')
        unique_id = uuid.uuid4().hex[:8]
        csv_filename = f"bitcoin_{timestamp}_{unique_id}.csv"

        # Criar uma pasta no S3 baseada na data (ano/mês/dia/hora/minuto/segundo)
        path_prefix = f"bitcoin/{pd.Timestamp.now().strftime('%Y/%m/%d')}/"
        csv_path = path_prefix + csv_filename

        # Baixar os dados minuto a minuto do BTC-USD
        btc = yf.download('BTC-USD', period='1d', interval='1m')
        ultimo_dado_df = btc.tail(1)

        # Processar e formatar os dados
        ultimo_dado_df.columns = ultimo_dado_df.columns.get_level_values(0)
        ultimo_dado_df.columns = [col.lower().replace(' ', '_') for col in ultimo_dado_df.columns]
        ultimo_dado_df = ultimo_dado_df.reset_index()

        ultimo_dado_df['open'] = ultimo_dado_df['open'].astype('float').round(2)
        ultimo_dado_df['high'] = ultimo_dado_df['high'].astype('float').round(2)
        ultimo_dado_df['low'] = ultimo_dado_df['low'].astype('float').round(2)
        ultimo_dado_df['close'] = ultimo_dado_df['close'].astype('float').round(2)
        ultimo_dado_df['volume'] = ultimo_dado_df['volume'].astype('Int64')
        ultimo_dado_df = ultimo_dado_df.rename(columns={'Datetime': 'date'})

        ultimo_dado_df = ultimo_dado_df[['date', 'open', 'high', 'low', 'close', 'volume']]

        # Salvar CSV em memória
        csv_buffer = StringIO()
        ultimo_dado_df.to_csv(csv_buffer, index=False)

        # Upload para S3 na pasta dinâmica
        s3.put_object(Bucket=bucket_name_raw, Key=csv_path, Body=csv_buffer.getvalue())

        logging.info(ultimo_dado_df)
        ultimo_dado_df['date'] = pd.to_datetime(ultimo_dado_df['date']).dt.date.astype(str)
        payload = ultimo_dado_df.to_dict(orient='records')[0]
        print(payload)
        return payload

    except Exception as e:
        logging.error(f"Erro ao obter valor do Bitcoin: {e}")
        return "Erro ao obter valor do Bitcoin"
