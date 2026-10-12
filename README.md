# LACEDA — Matching de Endereços CNEFE

Pipeline de Engenharia de Dados para processamento, indexação e correspondência de endereços da base CNEFE com uma base de consulta, visando identificar possíveis setores censitários associados aos registros.

## 1. Objetivo

O projeto busca automatizar a correspondência entre endereços de uma base de consulta e os registros disponíveis na base CNEFE.

O processo utiliza normalização de dados, indexação no Elasticsearch e estratégias de busca exata e aproximada. Quando não é possível identificar um único setor de acordo com as regras implementadas, o resultado é sinalizado para revisão.

Os resultados são exportados para arquivos que permitem analisar a distribuição das correspondências, investigar ambiguidades e acompanhar a execução do pipeline.

## 2. Estratégia utilizada

O pipeline está organizado nas seguintes etapas:

1. **Leitura dos dados:** carregamento dos arquivos da base CNEFE e da base de endereços a consultar.
2. **Preparação e normalização:** padronização de campos utilizados na correspondência, incluindo logradouro, CEP e número.
3. **Indexação:** seleção das colunas necessárias e envio dos registros da CNFE ao Elasticsearch em lotes, utilizando processamento por partições com PySpark.
4. **Busca exata:** comparação do logradouro normalizado e verificação de CEP ou número, conforme as regras implementadas.
5. **Busca aproximada:** utilização da busca textual do Elasticsearch com tolerância a diferenças na escrita, considerando o filtro municipal quando disponível.
6. **Classificação:** atribuição de um status conforme os resultados encontrados.
7. **Exportação e monitoramento:** geração dos arquivos de resultado, métricas e logs.

### 2.1. Estratégias de correspondência

**Busca exata**

A consulta compara o logradouro normalizado e utiliza CEP ou número como critério adicional. Quando os documentos retornados pertencem a um único setor, o resultado pode ser classificado como `MATCH_EXATO`, de acordo com as regras atuais.

Essa classificação representa o atendimento aos critérios implementados, mas não comprova, isoladamente, a correção real do endereço.

**Busca aproximada**

Quando a busca exata não retorna correspondência, a busca aproximada utiliza a consulta textual do Elasticsearch com tolerância a erros de escrita. CEP e número podem contribuir para a relevância dos documentos encontrados.

Os resultados são analisados para identificar setores candidatos. Quando existem candidatos que não podem ser confirmados com segurança, o registro é encaminhado para revisão.

### 2.2. Status dos resultados

| Status | Significado |
|---|---|
| `MATCH_EXATO` | Um único setor foi encontrado segundo as regras da busca exata. |
| `AMBIGUO` | Foram encontrados múltiplos setores na busca exata ou candidatos na busca aproximada que exigem revisão. |
| `NAO_ENCONTRADO` | Nenhum candidato foi retornado pelas estratégias de busca utilizadas. |

O campo `candidatos_setor` registra os setores identificados. O campo `scores_candidatos` armazena informações dos documentos retornados pela busca aproximada, incluindo suas pontuações de relevância.

As pontuações do Elasticsearch não representam probabilidades de acerto nem substituem a validação dos endereços.

## 3. Tecnologias utilizadas

- **Python:** implementação da lógica de correspondência, normalização e integração entre componentes.
- **PySpark:** processamento dos dados e envio dos registros ao Elasticsearch por partições.
- **Elasticsearch:** indexação e recuperação de documentos por meio de consultas exatas e textuais.
- **Pandas:** organização dos resultados em DataFrames.
- **Docker Compose:** inicialização e integração dos serviços utilizados pelo pipeline.

## 4. Estrutura esperada do projeto

Os arquivos de dados não são incluídos no pacote. A estrutura esperada para os dados locais é:

```text
data/
└── raw/
    ├── cnfe.csv
    └── base_busca.csv

reports/
src/
```

O diretório `src/` contém o código-fonte. O diretório `reports/` recebe os arquivos gerados durante a execução.

### Arquivos de entrada

- `cnfe.csv`: base de referência de endereços da CNFE.
- `base_busca.csv`: base com os endereços que serão consultados.

Os dois arquivos utilizam o separador `;` por padrão.

## 5. Requisitos e dependências

Antes de executar o projeto, verifique os seguintes requisitos:

- Docker Desktop instalado e em execução.
- Docker Compose habilitado.
- Arquivos de entrada disponíveis nos caminhos esperados.
- Configurações do serviço Elasticsearch compatíveis com o ambiente.
- Dependências Python instaladas na imagem ou no ambiente de execução utilizado pelo pipeline.

As bibliotecas utilizadas diretamente no código incluem `pandas` e `elasticsearch`. O processamento distribuído utiliza PySpark.

A versão exata das dependências deve ser consultada nos arquivos de configuração do projeto, como `requirements.txt`, `pyproject.toml` ou `Dockerfile`, conforme a estrutura disponível.

## 6. Instruções de execução

### 6.1. Preparar os arquivos

Coloque os arquivos de entrada no diretório:

```text
data/raw/
├── cnfe.csv
└── base_busca.csv
```

Confira se os nomes dos arquivos, o separador e as colunas esperadas correspondem às configurações do pipeline.

### 6.2. Iniciar o Elasticsearch

Na raiz do projeto, execute:

```bash
docker compose up -d elasticsearch
```

Aguarde até que o serviço esteja disponível antes de iniciar o processamento.

### 6.3. Executar o pipeline

Execute:

```bash
docker compose --profile pipeline run --rm pipeline
```

O pipeline realizará as etapas configuradas de processamento, indexação, correspondência e geração dos relatórios.

Os arquivos de saída serão gravados no diretório `reports/` do computador host, conforme a configuração do Docker Compose.

## 7. Arquivos gerados

| Arquivo | Descrição |
|---|---|
| `reports/logs.txt` | Registro cronológico das etapas, do progresso e dos erros da execução. |
| `reports/resultado_geocodificacao.csv` | Resultado da correspondência, preservando os registros de consulta. |
| `reports/relatorio_qualidade.json` | Validações e métricas consolidadas de qualidade operacional. |
| `reports/metricas_pipeline.json` | Métricas de processamento e distribuição dos resultados. |

A disponibilidade de cada arquivo depende de a respectiva etapa estar implementada e de a execução alcançar sua geração.

## 8. Métricas e monitoramento

O pipeline contempla métricas operacionais destinadas a acompanhar o processamento e identificar situações que precisam de investigação.

Entre as métricas previstas estão:

- Quantidade de registros de entrada e saída.
- Verificação da preservação dos registros.
- Quantidade e percentual de resultados por status.
- Quantidade e percentual de resultados por método de correspondência.
- Total de documentos indexados no Elasticsearch.
- Quantidade de consultas com candidatos.
- Média e máximo de candidatos por consulta.
- Quantidade de campos de entrada vazios.
- Tempo total de execução.

Essas métricas ajudam a monitorar o comportamento do pipeline, mas não medem, isoladamente, a acurácia da correspondência.

Para calcular métricas como precisão, revocação (*recall*) ou taxa real de acerto, é necessário comparar os resultados com uma base de referência validada manualmente.

## 9. Logs e diagnóstico

Os logs são gravados em `reports/logs.txt` e também podem ser exibidos no terminal.

Em caso de erro, a exceção e seu rastreamento (*traceback*) devem ser registrados, permitindo identificar a etapa em que ocorreu a falha.

Se o pipeline encerrar com erro, os arquivos gerados parcialmente não devem ser interpretados automaticamente como resultados completos.

### Verificações recomendadas

- Confirmar que os arquivos de entrada existem e podem ser lidos.
- Conferir o separador e os nomes das colunas.
- Verificar se o Elasticsearch está disponível.
- Conferir a quantidade de documentos indexados.
- Investigar registros com `AMBIGUO` ou `NAO_ENCONTRADO`.
- Comparar os campos dos candidatos com o endereço de entrada.

## 10. Configurações opcionais

As seguintes variáveis podem ser utilizadas para ajustar a execução, conforme a implementação do pipeline:

| Variável | Finalidade |
|---|---|
| `CNEFE_PATH` | Caminho do arquivo da base CNFE. |
| `SEARCH_PATH` | Caminho do arquivo da base de consulta. |
| `CNEFE_SEPARATOR` | Separador utilizado na leitura da CNFE; padrão `;`. |
| `DATA_DIR` | Diretório de dados. |
| `REPORTS_DIR` | Diretório de relatórios. |
| `ES_URL` | URL do Elasticsearch; padrão `http://localhost:9200`. |

Os valores efetivamente utilizados dependem da configuração do ambiente e da forma como as variáveis são consumidas pelo código e pelo Docker Compose.

## 11. Premissas adotadas

- Os arquivos de entrada estão disponíveis localmente e seguem os formatos configurados.
- Os campos necessários à correspondência estão presentes e podem ser interpretados pelo pipeline.
- A normalização reduz diferenças de formatação, mas não elimina todas as inconsistências semânticas dos dados.
- O Elasticsearch está disponível durante as etapas que dependem da indexação e da busca.
- Um único setor encontrado segundo as regras de correspondência não constitui, por si só, uma confirmação de acurácia.
- A avaliação da qualidade real depende de dados de referência confiáveis.

### Padronização do código municipal

Foi identificada uma diferença de formato entre as bases: a base de consulta pode apresentar códigos como `290070`, enquanto a CNFE pode apresentar `2900702`.

Essa divergência precisa ser tratada de forma consistente para que o filtro municipal funcione corretamente. A adoção de um campo auxiliar normalizado depende da validação do padrão dos códigos e da atualização dos documentos indexados.

## 12. Limitações conhecidas

- **Ambiguidade:** endereços semelhantes podem estar associados a setores diferentes.
- **Qualidade dos dados de origem:** campos vazios, inconsistentes ou incorretos podem prejudicar a correspondência.
- **Pontuação de relevância:** o score do Elasticsearch ordena resultados segundo critérios de busca, mas não representa uma probabilidade de acerto.
- **Busca aproximada:** tolerar erros de escrita aumenta a possibilidade de recuperar candidatos, mas também pode retornar registros incorretos.
- **Dependência da indexação:** alterações no código de indexação ou no mapeamento dos campos podem exigir a reconstrução ou atualização do índice.
- **Validação de acurácia:** as métricas operacionais não substituem testes com endereços previamente conferidos.
- **Formato dos códigos municipais:** diferenças entre as bases precisam ser normalizadas sem eliminar informações necessárias para a identificação correta do município.

## 13. Próximos passos

- Validar a padronização dos códigos municipais.
- Testar casos de correspondência exata, aproximada, ambígua e não encontrada.
- Conferir manualmente uma amostra dos resultados.
- Avaliar as regras de classificação e a relevância dos candidatos.
- Medir a precisão e a revocação com uma base de referência validada.
- Documentar os resultados dos testes e atualizar as limitações conforme a evolução do projeto.

## 14. Dados e índice

Os arquivos de dados e o índice Elasticsearch não estão incluídos no ZIP do projeto.

Conforme a configuração atual informada, o índice é recriado pelo pipeline a cada execução. Portanto, a indexação precisa ser executada novamente antes da busca.

**Observação:** confirme o comportamento de recriação do índice no código e na configuração de execução antes de modificar ou reutilizar um índice existente.