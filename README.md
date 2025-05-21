# Previsão de Valorização de 5% do Bitcoin

Este projeto desenvolve um modelo de machine learning para prever se o preço do Bitcoin terá uma valorização de pelo menos 5% no próximo dia. O foco é identificar "bons dias de entrada" no mercado, com base em dados históricos de mercado e técnicas modernas de aprendizado de máquina.

## Objetivo

Prever, com base em dados históricos de preços e indicadores, se o preço do Bitcoin subirá pelo menos 5% em relação ao fechamento do dia anterior.

## Estrutura do Projeto

- Desenvolvimento_Modelo-target5pct.ipynb: Notebook principal com o pipeline de desenvolvimento do modelo.
- config_classifier.yaml: Arquivo de configuração para os hiperparâmetros do classificador.

## Seleção de Variáveis e Modelo

### Seleção de Variáveis com RFECV

*RFECV* (Recursive Feature Elimination with Cross-Validation) é uma técnica de seleção de variáveis que busca identificar automaticamente o subconjunto de atributos mais relevantes para o modelo, eliminando os menos importantes de forma iterativa.

- Como funciona:
1. Um estimador é treinado com todas as variáveis.
2. A importância de cada variável é avaliada.
3. A variável menos importante é removida.
4. O processo se repete com validação cruzada a cada etapa.
5. A combinação ideal de variáveis é aquela com melhor desempenho médio.

Vantagens:
- Redução de overfitting.
- Melhor interpretabilidade.
- Menor tempo de treinamento.

## Modelo de Classificação com LightGBM

*LightGBM* é um algoritmo de aprendizado de máquina baseado em árvores de decisão que utiliza gradient boosting. Ele é eficiente e escalável, especialmente para conjuntos de dados com muitas variáveis.

Principais características:
- Crescimento por folhas (leaf-wise) para maior precisão.
- Suporte a valores faltantes e variáveis categóricas.
- Alta velocidade de treino e baixo consumo de memória.

Por que usar LightGBM:
- Ótimo desempenho em classificações binárias desbalanceadas.
- Suporte nativo a importância de variáveis, ideal para RFECV.

## Dados

### Descrição das Variáveis

Variável                   | Descrição
-------------------------- | -----------------------------------------
Date                       | Data da observação
Open                       | Preço de abertura do Bitcoin naquele dia
High                       | Preço máximo do dia
Low                        | Preço mínimo do dia
Close                      | Preço de fechamento do dia
Volume                     | Volume negociado
log_return_1d              | Retorno logarítmico diário do preço
rolling_mean_{n}           | Média móvel dos últimos n dias
rolling_std_{n}            | Desvio padrão móvel dos últimos n dias
rolling_max_{n}            | Máximo valor nos últimos n dias
rolling_min_{n}            | Mínimo valor nos últimos n dias
zscore_{n}                 | Z-score baseado na média e desvio móvel
momentum_3d, momentum_7d   | Diferença com o preço de 3/7 dias atrás
rsi_14                     | Índice de Força Relativa com janela de 14 dias
volume_spike               | Razão entre volume atual e média de 7 dias
position_in_range_30       | Posição do preço no intervalo dos últimos 30 dias
target                     | 1 se o preço subir ≥5% nos próximos 3 dias, senão 0

## Bibliotecas Principais

- pandas, numpy>=1.26.0
- scikit-learn==1.6.1
- lightgbm==4.6.0
- seaborn, matplotlib
- boto3
- yaml

## Como Executar

1. Clone o repositório:
   git clone https://github.com/thaisrcdias/fiap_ml_bitcoin
   cd fiap_ml_bitcoin

2. Instale as dependências:
   ```bash
   pip install -r ml/requirements.txt
   ```
3. Execute o notebook:
   Abra o arquivo ml/notebook/Desenvolvimento_Modelo-target5pct.ipynb no Jupyter ou VSCode.
4. Execução no Glue:
    O  arquivo ml/glue/
    - parametros de exemplo: 
        - --additional-python-modules lightgbm==4.6.0, scikit-learn==1.6.1, numpy>=1.26.0, awswrangler
        - --Open 22929.626953125
        - --High 23134.01171875
        - --Low 22549.744140625
        - --Close 22636.46875
        - --Volume 26405069715
        - --model_name model_bitcoin_5pct.pkl
        - --bucket_name fiap-files-512988434617

## Resultados Esperados

O modelo foi treinado para identificar padrões históricos que precedem valorização significativa do BTC. Os resultados são avaliados principalmente por sua capacidade de reduzir falsos positivos em cenários altamente desbalanceados.

## Observações

- Este projeto é voltado para fins educacionais. Não constitui recomendação de investimento.
- É necessário acesso à AWS S3 com permissões adequadas.