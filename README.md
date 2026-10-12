# LACEDA — Matching de endereços CNEFE

## Estrutura esperada

Coloque os arquivos de dados localmente (não são incluídos neste pacote):

```text
data/
  raw/
    cnfe.csv
    base_busca.csv
reports/
src/
```

O `cnfe.csv` é lido com separador `;` por padrão. A base `base_busca.csv` é lida com separador `;`.

## Executar com Docker Compose

Requisitos: Docker Desktop com Docker Compose habilitado.

1. Deixe `cnfe.csv` e `base_busca.csv` em `data/raw/`.
2. Inicie o Elasticsearch:

   ```bash
   docker compose up -d elasticsearch
   ```

3. Execute o pipeline:

   ```bash
   docker compose --profile pipeline run --rm pipeline
   ```

Os arquivos de saída serão gravados em `reports/` no computador host.

## Arquivos gerados

- `reports/logs.txt`: log cronológico com etapas, progresso do matching e erros com rastreamento.
- `reports/resultado_geocodificacao.csv`: resultado do matching, preservando as consultas de entrada.
- `reports/relatorio_qualidade.json`: validações e métricas consolidadas.
- `reports/metricas_pipeline.json`: métricas de processamento e distribuição dos resultados.

## Métricas calculadas

- Quantidade de registros de entrada e saída e verificação de preservação dos registros.
- Quantidade e percentual por status (`MATCH_EXATO`, `AMBIGUO`, `NAO_ENCONTRADO`).
- Quantidade e percentual por método de matching.
- Total de documentos indexados no Elasticsearch.
- Quantidade de consultas com candidatos, média e máximo de candidatos por consulta.
- Campos de entrada vazios e tempo total da execução.

Essas métricas descrevem o comportamento do pipeline; não são, por si só, uma medida de acurácia. Para calcular precisão/recall ou taxa de acerto real, é necessário ter uma base de referência validada manualmente.

## Logs e diagnóstico

O log é gravado em `reports/logs.txt` e também aparece no terminal. Em caso de erro, a exceção é registrada com traceback e o pipeline encerra com falha, em vez de apresentar a execução como concluída.

## Configurações opcionais

As variáveis `CNEFE_PATH`, `SEARCH_PATH`, `CNEFE_SEPARATOR`, `DATA_DIR`, `REPORTS_DIR` e `ES_URL` permitem alterar caminhos, separador e URL do Elasticsearch. O padrão de `CNEFE_SEPARATOR` é `;`.

**Observação:** os arquivos de dados e o índice Elasticsearch não estão incluídos no ZIP. O índice é recriado pelo pipeline a cada execução.
