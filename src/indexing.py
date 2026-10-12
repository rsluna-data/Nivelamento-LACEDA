
import hashlib
import json
import os

from elasticsearch import Elasticsearch, helpers


INDEX_NAME = "cnefe-enderecos-v1"


def get_client():
    return Elasticsearch(
        os.getenv("ES_URL", "http://localhost:9200"),
        request_timeout=60,
    )



def create_index(client, index_name=INDEX_NAME):
    if client.indices.exists(index=index_name):
        return

    mappings = {
        "properties": {
            "codigo_endereco": {"type": "keyword"},
            "codigo_municipio": {"type": "keyword"},
            "codigo_setor": {"type": "keyword"},
            "cep_normalizado": {"type": "keyword"},
            "numero_normalizado": {"type": "keyword"},
            "logradouro_normalizado": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "endereco_normalizado": {"type": "text"},
            "localidade_normalizada": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
        }
    }

    client.indices.create(
        index=index_name,
        mappings=mappings,
    )


def _index_partition(rows, index_name):
    """
    Executada por partição Spark.
    Cada partição envia documentos em lotes ao Elasticsearch.
    """
    client = get_client()

    def actions():
        for row in rows:
            data = row.asDict(recursive=True)

            document = {
                "codigo_endereco": data.get(
                    "cod_unico_endereco"
                ),
                "codigo_municipio": data.get(
                    "cod_municipio"
                ),
                "codigo_setor": data.get("cod_setor"),
                "cep_normalizado": data.get(
                    "cep_normalizado"
                ),
                "numero_normalizado": data.get(
                    "numero_normalizado"
                ),
                "logradouro_normalizado": data.get(
                    "logradouro_normalizado"
                ),
                "endereco_normalizado": data.get(
                    "endereco_normalizado"
                ),
                "localidade_normalizada": data.get(
                    "localidade_normalizada"
                ),
            }

            # ID determinístico baseado no conteúdo do documento.
            raw = json.dumps(
                data,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )

            document_id = hashlib.sha256(
                raw.encode("utf-8")
            ).hexdigest()

            yield {
                "_op_type": "index",
                "_index": index_name,
                "_id": document_id,
                "_source": document,
            }

    try:
        for success, item in helpers.streaming_bulk(
            client,
            actions(),
            chunk_size=500,
            raise_on_error=True,
        ):
            if not success:
                raise RuntimeError(
                    f"Falha na indexação: {item}"
                )
    finally:
        client.close()


def index_dataframe(df, index_name=INDEX_NAME):
    client = get_client()

    try:
        create_index(client, index_name)
    finally:
        client.close()

    # Limita as colunas transferidas ao Elasticsearch.
    columns = [
    "cod_unico_endereco",
    "cod_municipio",
    "cod_setor",
    "cep_normalizado",
    "numero_normalizado",
    "logradouro_normalizado",
    "endereco_normalizado",
    "localidade_normalizada",
]

    indexed_df = df.select(*columns)

    indexed_df.foreachPartition(
        lambda rows: _index_partition(rows, index_name)
    )

    client = get_client()

    try:
        client.indices.refresh(index=index_name)

        count = client.count(index=index_name)["count"]
        print(f"Documentos indexados: {count}")

        return count
    finally:
        client.close()
