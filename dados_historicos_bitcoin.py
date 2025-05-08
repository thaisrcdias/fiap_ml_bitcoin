import yfinance as yf
from io import BytesIO
import pandas as pd
import os
import boto3

bucket_name_raw = "raw-209112358514"
parquet_filename = f"df_historico_bitcoin_{pd.Timestamp.now().strftime('%Y-%m-%d')}.parquet"

s3 = boto3.client(
    "s3",
    aws_access_key_id= os.getenv('ACCESS_KEY_ID'), 
    aws_secret_access_key= os.getenv('SECRET_ACCESS_KEY'),
    region_name="us-east-1"
)

btc = yf.Ticker("BTC-USD")
df_hist = btc.history(period="max")

# 2. Renomear colunas: tudo minúsculo e _ no lugar de espaços
df_hist.columns = [col.lower().replace(' ', '_') for col in df_hist.columns]

# 3. Resetar o índice (timestamp -> coluna)
df_hist = df_hist.reset_index()

# 4. Transformar a coluna 'Date' para tipo date (descarta hora)
df_hist['date'] = df_hist['Date'].dt.date
df_hist = df_hist.drop(columns=['Date'])
print(df_hist.dtypes)
print(df_hist.head(5))

# 5. Ajustar tipos:
df_hist['open'] = df_hist['open'].astype('float').round(2)
df_hist['high'] = df_hist['high'].astype('float').round(2)
df_hist['low'] = df_hist['low'].astype('float').round(2)
df_hist['close'] = df_hist['close'].astype('float').round(2)
df_hist['volume'] = df_hist['volume'].astype('Int64')
df_hist['dividends'] = df_hist['dividends'].astype('Int64')
df_hist['stock_splits'] = df_hist['stock_splits'].astype('Int64')

# # 6. Ordenar colunas na ordem desejada
# df_hist = df_hist[['date', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits']]

# buffer = BytesIO()
# df_hist.to_parquet(buffer, index=True)

# # Caminho no S3 (particionado pela data)
# parquet_path = f"historico/bitcoin/{parquet_filename}"
# buffer.seek(0)
# s3.upload_fileobj(buffer, bucket_name_raw, parquet_path)

# print(f"Upload de {parquet_filename} concluído em {parquet_path}")

