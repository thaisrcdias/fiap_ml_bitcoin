import yfinance as yf
import pandas as pd
from io import BytesIO

from fastapi import APIRouter
from fastapi import Response
import logging
import boto3
import os

from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s:%(funcName)s:%(message)s")

router = APIRouter()
bucket_name_raw = "raw-209112358514"
parquet_filename = f"bitcoin_{pd.Timestamp.now().strftime('%Y-%m-%d')}.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id= os.getenv('ACCESS_KEY_ID'), 
    aws_secret_access_key= os.getenv('SECRET_ACCESS_KEY'),
    region_name="us-east-1"
)


   
@router.get("/bitcoin_value")
def get_value_bitcoin() -> Response:

    try:
        # Baixar os dados minuto a minuto do BTC-USD
        btc = yf.download('BTC-USD', period='1d', interval='1m')

        # Pegar o último registro como DataFrame
        ultimo_dado_df = btc.tail(1)

        # Achatar as colunas (remover multiindex)
        ultimo_dado_df.columns = ultimo_dado_df.columns.get_level_values(0)
        logging.info(ultimo_dado_df.columns)

        # 2. Renomear colunas: tudo minúsculo e _ no lugar de espaços
        ultimo_dado_df.columns = [col.lower().replace(' ', '_') for col in ultimo_dado_df.columns]

        # 3. Resetar o índice (timestamp -> coluna)
        ultimo_dado_df = ultimo_dado_df.reset_index()

        # 5. Ajustar tipos:
        ultimo_dado_df['open'] = ultimo_dado_df['open'].astype('float').round(2)
        ultimo_dado_df['high'] = ultimo_dado_df['high'].astype('float').round(2)
        ultimo_dado_df['low'] = ultimo_dado_df['low'].astype('float').round(2)
        ultimo_dado_df['close'] = ultimo_dado_df['close'].astype('float').round(2)
        ultimo_dado_df['volume'] = ultimo_dado_df['volume'].astype('Int64')
        ultimo_dado_df = ultimo_dado_df.rename(columns={'Datetime': 'date'})

        logging.info(ultimo_dado_df.head(1))
        # 6. Ordenar colunas na ordem desejada
        ultimo_dado_df = ultimo_dado_df[['date', 'open', 'high', 'low', 'close', 'volume']]

        buffer = BytesIO()
        ultimo_dado_df.to_parquet(buffer, index=True)
        # Caminho no S3 (particionado pela data)
        parquet_path = f"bitcoin/{parquet_filename}"
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket_name_raw, parquet_path)

        logging.info(ultimo_dado_df)
        ultimo_dado_df['date'] = pd.to_datetime(ultimo_dado_df['date']).dt.date.astype(str)
        # Converter para dict
        payload = ultimo_dado_df.to_dict(orient='records')[0]
        print(payload)


        return JSONResponse(content=payload, status_code=200)

    except Exception as e:
        logging.error(f"Erro ao obter valor do Bitcoin: {e}")
        return JSONResponse(content={"detail": str(e)}, status_code=500)
