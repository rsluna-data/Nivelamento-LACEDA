
import re

import pandas as pd
from elasticsearch import Elasticsearch

from src.preprocessing import normalize_street, normalize_text
from src.indexing import get_client, INDEX_NAME


def normalize_cep(value):
    """Mantém somente os números do CEP."""
    return re.sub(r"[^0-9]", "", str(value or ""))


def normalize_number(value):
    """Padroniza o número do endereço."""
    value = normalize_text(value)

    if not value or value in {"SN", "S N", "S/N", "SEM NUMERO"}:
        return "SN" if value else ""

    return value


def normalize_municipio(value):
    """Normaliza o código do município."""
    value = str(value or "").strip()

    if not value:
        return ""

    # Evita transformar um código numérico em número decimal.
    if value.endswith(".0") and value[:-2].isdigit():
        value = value[:-2]

    return value


def extract_sectors(hits):
    """Extrai setores únicos dos documentos encontrados."""
    return sorted({
        str(hit["_source"]["codigo_setor"])
        for hit in hits
        if hit["_source"].get("codigo_setor")
    })


def search_address(
    client: Elasticsearch,
    logradouro: str,
    cep: str,
    numero: str,
    municipio="",
    index_name: str = INDEX_NAME,
):
    logradouro_norm = normalize_street(logradouro)
    cep_norm = normalize_cep(cep)
    numero_norm = normalize_number(numero)
    municipio_norm = normalize_municipio(municipio)

    if not logradouro_norm:
        return {
            "codigo_setor": None,
            "status_match": "NAO_ENCONTRADO",
            "metodo_match": "SEM_LOGRADOURO",
            "candidatos_setor": [],
            "quantidade_documentos": 0,
            "scores_candidatos": "",
        }

    # Filtros geográficos compartilhados pelas buscas.
    filters = []

    if municipio_norm:
        filters.append({
            "term": {
                "codigo_municipio_busca": municipio_norm
            }
        })

    # 1. Busca exata: logradouro + CEP ou número.
    should = []

    if cep_norm:
        should.append({
            "term": {"cep_normalizado": cep_norm}
        })

    if numero_norm:
        should.append({
            "term": {"numero_normalizado": numero_norm}
        })

    if should:
        response = client.search(
            index=index_name,
            size=100,
            query={
                "bool": {
                    "filter": filters + [
                        {
                            "term": {
                                "logradouro_normalizado.keyword":
                                    logradouro_norm
                            }
                        }
                    ],
                    "should": should,
                    "minimum_should_match": 1,
                }
            },
        )

        hits = response["hits"]["hits"]
        setores = extract_sectors(hits)

        if len(setores) == 1:
            return {
                "codigo_setor": setores[0],
                "status_match": "MATCH_EXATO",
                "metodo_match": "LOGRADOURO_EXATO_CEP_OU_NUMERO",
                "candidatos_setor": setores,
                "quantidade_documentos": len(hits),
                "scores_candidatos": "",
            }

        if len(setores) > 1:
            return {
                "codigo_setor": None,
                "status_match": "AMBIGUO",
                "metodo_match": "EXATO_MULTIPLOS_SETORES",
                "candidatos_setor": setores,
                "quantidade_documentos": len(hits),
                "scores_candidatos": "",
            }

    # 2. Busca aproximada: município, quando disponível,
    # e todos os termos do logradouro, com tolerância a erros.
    query = {
        "bool": {
            "filter": filters,
            "must": [
                {
                    "match": {
                        "logradouro_normalizado": {
                            "query": logradouro_norm,
                            "operator": "and",
                            "fuzziness": "AUTO",
                        }
                    }
                }
            ],
        }
    }

    # CEP e número aumentam a relevância, mas não são obrigatórios.
    should = []

    if cep_norm:
        should.append({
            "term": {"cep_normalizado": cep_norm}
        })

    if numero_norm:
        should.append({
            "term": {"numero_normalizado": numero_norm}
        })

    if should:
        query["bool"]["should"] = should

    response = client.search(
        index=index_name,
        size=100,
        query=query,
    )

    hits = response["hits"]["hits"]
    setores = extract_sectors(hits)

    scores = [
        {
            "codigo_setor": hit["_source"].get("codigo_setor"),
            "score": hit.get("_score"),
            "logradouro": hit["_source"].get("logradouro_normalizado"),
            "cep": hit["_source"].get("cep_normalizado"),
            "numero": hit["_source"].get("numero_normalizado"),
            "municipio": hit["_source"].get("codigo_municipio"),
        }
        for hit in hits
    ]

    scores_json = __import__("json").dumps(
        scores,
        ensure_ascii=False,
    )

    if setores:
        return {
            "codigo_setor": None,
            "status_match": "AMBIGUO",
            "metodo_match": "BUSCA_APROXIMADA_REVISAO",
            "candidatos_setor": setores,
            "quantidade_documentos": len(hits),
            "scores_candidatos": scores_json,
        }

    return {
        "codigo_setor": None,
        "status_match": "NAO_ENCONTRADO",
        "metodo_match": "SEM_CANDIDATOS_APROXIMADOS",
        "candidatos_setor": [],
        "quantidade_documentos": 0,
        "scores_candidatos": "",
    }


def match_dataframe(df, index_name=INDEX_NAME):
    client = get_client()
    results = []

    try:
        for _, row in df.iterrows():
            result = search_address(
                client=client,
                logradouro=row.get("consulta_logradouro", ""),
                cep=row.get("consulta_cep", ""),
                numero=row.get("consulta_numero", ""),
                municipio=row.get("consulta_municipio", ""),
                index_name=index_name,
            )
            results.append(result)
    finally:
        client.close()

    match_results = pd.DataFrame(results)
    output = df.copy()

    for column in match_results.columns:
        output[column] = match_results[column].values

    output["candidatos_setor"] = output["candidatos_setor"].apply(
        lambda values: ";".join(values)
        if isinstance(values, list)
        else ""
    )

    return output
