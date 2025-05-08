import json
import os

import uvicorn
from routes import dados_streaming_bitcoin
from fastapi import FastAPI
from fastapi import Response


app = FastAPI()

app.include_router(dados_streaming_bitcoin.router)

@app.get("/")
async def root() -> Response:
    return Response(content=json.dumps({"Status": "OK"}), media_type="application/json", status_code=200)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))