import yfinance as yf
from io import BytesIO
import pandas as pd
import os
import boto3

bucket_name_raw = "raw-209112358514"
parquet_filename = f"historico_bitcoin_{pd.Timestamp.now().strftime('%Y-%m-%d')}.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id= os.getenv('MY_SECRET_KEY'), 
    aws_secret_access_key= os.getenv('MY_SECRET_ACCESS_KEY'),
    region_name="us-east-1"
)

btc = yf.Ticker("BTC-USD")
df_hist = btc.history(period="max")
buffer = BytesIO()
df_hist.to_parquet(buffer, engine='pyarrow', index=False)

# Caminho no S3 (particionado pela data)
parquet_path = f"historico/bitcoin/{parquet_filename}"
buffer.seek(0)
s3.upload_fileobj(buffer, bucket_name_raw, parquet_path)
print(f"Upload de {parquet_filename} concluído em {parquet_path}")

