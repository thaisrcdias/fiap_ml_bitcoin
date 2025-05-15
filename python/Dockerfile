FROM python:3.11-slim

# Instalar o zip
RUN apt-get update && apt-get install -y zip && rm -rf /var/lib/apt/lists/*

# Criar diretório para a Lambda Layer
RUN mkdir -p /layer/python

# Copiar o arquivo requirements.txt e instalar as dependências no diretório /layer/python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt -t /layer/python

# Compactar a Layer em um arquivo .zip
WORKDIR /layer
RUN zip -r /layer/layer.zip python