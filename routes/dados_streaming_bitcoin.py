import yfinance as yf
import pandas as pd
from io import BytesIO

from fastapi import APIRouter
from fastapi import Response
import logging

from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s:%(funcName)s:%(message)s")

router = APIRouter()
   
@router.get("/bitcoin_value")
def get_value_bitcoin() -> Response:

    # Baixar os dados minuto a minuto do BTC-USD
    btc = yf.download('BTC-USD', period='1d', interval='1m')

    # Pegar o último registro como DataFrame
    ultimo_dado_df = btc.tail(1)

    # Achatar as colunas (remover multiindex)
    ultimo_dado_df.columns = ultimo_dado_df.columns.get_level_values(0)

    # Resetar o índice para transformar datetime em coluna
    ultimo_dado_df = ultimo_dado_df.reset_index()

    # 2. Renomear colunas: tudo minúsculo e _ no lugar de espaços
    ultimo_dado_df.columns = [col.lower().replace(' ', '_') for col in ultimo_dado_df.columns]

    # 3. Resetar o índice (timestamp -> coluna)
    ultimo_dado_df = ultimo_dado_df.reset_index()


    # 5. Ajustar tipos:
    ultimo_dado_df['open'] = ultimo_dado_df['open'].astype('float').round(0).astype('Int64')
    ultimo_dado_df['high'] = ultimo_dado_df['high'].astype('float').round(0).astype('Int64')
    ultimo_dado_df['low'] = ultimo_dado_df['low'].astype('float').round(0).astype('Int64')
    ultimo_dado_df['close'] = ultimo_dado_df['close'].astype('float').round(0).astype('Int64')
    ultimo_dado_df['volume'] = ultimo_dado_df['volume'].astype('Int64')

    # 6. Ordenar colunas na ordem desejada
    ultimo_dado_df = ultimo_dado_df[['datetime', 'open', 'high', 'low', 'close', 'volume']]

    buffer = BytesIO()
    ultimo_dado_df.to_parquet(buffer, index=True)
    print(ultimo_dado_df)
