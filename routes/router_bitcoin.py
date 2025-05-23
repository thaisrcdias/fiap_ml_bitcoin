from fastapi import APIRouter
from fastapi import Response
from datetime import datetime
from fastapi import Query
import boto3
import logging
import pandas as pd
from io import BytesIO
from fastapi.responses import JSONResponse
from utils.bitcoin_payload import BitcoinPayload
import awswrangler as wr


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s:%(funcName)s:%(message)s")

router = APIRouter()

@router.post("/calculate_prediction")
def start_glue_job(payload: BitcoinPayload):
    """
    Recebe dados via POST e dispara um Glue Job na AWS.
    """
    try:
        glue_job_name = "model-bitcoin"
        glue = boto3.client("glue", region_name='us-east-1')

        # Passar os dados como argumentos para o Glue Job (como JSON)
        args = {
            '--input_data': payload.json()
        }

        response = glue.start_job_run(
            JobName=glue_job_name,
            Arguments=args
        )

        return JSONResponse(content={"message": f"Glue Job iniciado com a data {payload.Date}", "job_run_id": response["JobRunId"]})

    except Exception as e:
        logging.error(f"Erro ao iniciar Glue Job: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
   
@router.get("/get_bitcoin_prediction")
def get_bitcoin_prediction(
    data: str = Query(description="Data no formato: dd/MM/yyyy HH:mm")
) -> Response:
    """
    Endpoint para obter a previsão do valor do Bitcoin.
    :param data: Data no formato: dd/MM/yyyy HH:mm
    :return: JSON com a previsão do valor do Bitcoin.
    """
    try:
        # Verifica se a data está no formato correto
        dt = datetime.strptime(data, "%d/%m/%Y %H:%M")
      
        year = dt.year
        month = f"{dt.month:02d}"
        day = f"{dt.day:02d}"
        hora = f"{dt.hour:02d}"
        minuto = f"{dt.minute:02d}"
        boto3_session = boto3.Session(region_name="us-east-1")
        
        query = f"SELECT * FROM tbl_results_model where ano='{year}' and mes='{month}' and dia='{day}' and hora='{hora}' and minuto='{minuto}' "
        print(query)
        df = wr.athena.read_sql_query(sql=query, database="db_source_bitcoin", workgroup='workgroup_analytics', boto3_session=boto3_session)
        
        # df['data'] = pd.to_datetime(df['data'])
        row = df

        if row.empty:
            return JSONResponse(status_code=404, content={"error": "Data não encontrada no arquivo."})

        # Montar o dicionário de resposta com as colunas desejadas
        result = {
            "Volume": float(row.iloc[0]["volume"]),
            "log_return_1d": float(row.iloc[0].get("log_return_1d", 0.0)),
            "zscore_7": float(row.iloc[0].get("zscore_7", 0.0)),
            "rolling_std_30": float(row.iloc[0].get("rolling_std_30", 0.0)),
            "momentum_3d": float(row.iloc[0].get("momentum_3d", 0.0)),
            "rsi_14": float(row.iloc[0].get("rsi_14", 0.0)),
            "score": float(row.iloc[0].get("score", 0.0))
        }

        return JSONResponse(content=result)

    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Data inválida. O formato correto é: dd/MM/yyyy HH:mm"})
    except Exception as e:
        logging.error(f"Erro ao obter valor do Bitcoin: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})