
import re

from elasticsearch import Elasticsearch

from src.preprocessing import normalize_street, normalize_text
from src.indexing import get_client, INDEX_NAME


def normalize_cep(value):
    """Mantém somente os números do CEP."""
    return re.sub(r"[^0-9]", "", str(value or ""))


def normalize_number(value):
    """Padroniza o número do endereço, incluindo casos sem número."""
    value = normalize_text(value)

    if not value or value in {"SN", "S N", "S/N", "SEM NUMERO"}:
        return "SN" if value else ""

    return value


def search_address(
    client: Elasticsearch,
    logradouro: str,
    cep: str,
    numero: str,
    index_name: str = INDEX_NAME,
):
    logradouro_norm = normalize_street(logradouro)
    cep_norm = normalize_cep(cep)
    numero_norm = normalize_number(numero)

    if not logradouro_norm:
        return {
            "codigo_setor": None,
            "status_match": "NAO_ENCONTRADO",
            "metodo_match": "SEM_LOGRADOURO",
            "candidatos_setor": [],
        }

    # 1. Busca exata pelo logradouro e por CEP OU número.
    # Não exige que CEP e número estejam ambos corretos.
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
                    "filter": [
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

        setores = sorted({
            hit["_source"]["codigo_setor"]
            for hit in hits
            if hit["_source"].get("codigo_setor")
        })

        if len(setores) == 1:
            return {
                "codigo_setor": setores[0],
                "status_match": "MATCH_EXATO",
                "metodo_match": "LOGRADOURO_EXATO_CEP_OU_NUMERO",
                "candidatos_setor": setores,
            }

        if len(setores) > 1:
            return {
                "codigo_setor": None,
                "status_match": "AMBIGUO",
                "metodo_match": "EXATO_MULTIPLOS_SETORES",
                "candidatos_setor": setores,
            }

    # 2. Busca aproximada para levantar candidatos.
    # O resultado não recebe setor automaticamente.
    response = client.search(
        index=index_name,
        size=20,
        query={
            "match": {
                "logradouro_normalizado": {
                    "query": logradouro_norm,
                    "fuzziness": "AUTO",
                }
            }
        },
    )

    hits = response["hits"]["hits"]

    setores = sorted({
        hit["_source"]["codigo_setor"]
        for hit in hits
        if hit["_source"].get("codigo_setor")
    })

    if setores:
        return {
            "codigo_setor": None,
            "status_match": "AMBIGUO",
            "metodo_match": "BUSCA_APROXIMADA_REVISAO",
            "candidatos_setor": setores,
        }

    return {
        "codigo_setor": None,
        "status_match": "NAO_ENCONTRADO",
        "metodo_match": "SEM_CANDIDATOS",
        "candidatos_setor": [],
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
                index_name=index_name,
            )
            results.append(result)
    finally:
        client.close()

    import pandas as pd

    match_results = pd.DataFrame(results)
    output = df.copy()

    for column in match_results.columns:
        output[column] = match_results[column].values

    output["candidatos_setor"] = output[
        "candidatos_setor"
    ].apply(
        lambda values: ";".join(values)
        if isinstance(values, list)
        else ""
    )

    return output