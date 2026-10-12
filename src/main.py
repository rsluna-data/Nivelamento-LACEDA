
# from pathlib import Path
# import pandas as pd

# PASTA = Path("data")

# for arquivo in PASTA.iterdir():
#     if not arquivo.is_file():
#         continue

#     extensao = arquivo.suffix.lower()

#     if extensao == ".csv" and "cnfe" in arquivo.name.lower():
#         print("Arquivo:", arquivo.name)
#         print("Tamanho (MB):", round(
#             arquivo.stat().st_size / 1024**2, 2
#         ))

#         df = pd.read_csv(
#             arquivo,
#             sep=None,
#             engine="python",
#             encoding="utf-8-sig",
#             nrows=5,
#             dtype=str
#         )

#         print("\nColunas:")
#         print(df.columns.tolist())

#         print("\nAmostra:")
#         print(df.head(3).to_string(index=False))

#     elif extensao == ".parquet" and "cnefe" in arquivo.name.lower():
#         df = pd.read_parquet(arquivo)
#         print("Arquivo:", arquivo.name)
#         print("Colunas:", df.columns.tolist())
#         print(df.head(3).to_string(index=False))

#     elif extensao in [".xlsx", ".xls"] and "cnefe" in arquivo.name.lower():
#         print("Arquivo Excel:", arquivo.name)
#         df = pd.read_excel(arquivo, nrows=5, dtype=str)
#         print("Colunas:", df.columns.tolist())
#         print(df.head(3).to_string(index=False))

# from pathlib import Path
# import csv

# # Localiza a raiz do projeto
# raiz_projeto = Path(__file__).resolve().parent.parent

# # Caminho correto do CSV
# arquivo = raiz_projeto / "data" / "raw" / "cnfe.csv"

# print("Arquivo encontrado:", arquivo.exists())

# # Lê uma pequena amostra para identificar o separador
# with open(arquivo, "r", encoding="utf-8-sig", newline="") as f:
#     amostra = f.read(10000)

#     print("\nInício do arquivo:")
#     print(amostra[:500])

#     try:
#         separador = csv.Sniffer().sniff(
#             amostra, delimiters=",;"
#         ).delimiter

#         print("\nSeparador identificado:", repr(separador))

#     except csv.Error:
#         print("\nNão foi possível identificar o separador automaticamente.")

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession

from src.preprocessing import (
    read_cnefe,
    prepare_cnefe,
    read_search_base,
    save_prepared_data,
)
from src.indexing import (
    get_client,
    create_index,
    index_dataframe,
    INDEX_NAME,
)
from src.matching import match_dataframe
from src.validation import validate_results


BASE_DIR = Path("/app/data")
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
REPORTS_DIR = Path("/app/reports")


def main():
    cnefe_path = Path(
        os.getenv(
            "CNEFE_PATH",
            str(RAW_DIR / "cnfe.csv"),
        )
    )

    search_path = Path(
        os.getenv(
            "SEARCH_PATH",
            str(RAW_DIR / "base_busca.csv"),
        )
    )

    separator = os.getenv("CNEFE_SEPARATOR", ";")

    if not cnefe_path.exists():
        raise FileNotFoundError(
            f"CNEFE não encontrado: {cnefe_path}"
        )

    if not search_path.exists():
        raise FileNotFoundError(
            f"Base de busca não encontrada: {search_path}"
        )

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("CNEFE-Address-Linkage")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Ler e preparar o CNEFE
        print("\n[1/6] Lendo CNEFE...")

        cnefe = read_cnefe(
            spark,
            str(cnefe_path),
            separator=separator,
        )

        print("Colunas encontradas:", len(cnefe.columns))

        print("\n[2/6] Preparando os endereços...")

        prepared = prepare_cnefe(cnefe)

        prepared_path = PROCESSED_DIR / "cnefe_prepared"

        save_prepared_data(
            prepared,
            str(prepared_path),
        )

        print("Base preparada salva em:", prepared_path)

        # 2. Ler a base de busca
        print("\n[3/6] Lendo base de busca...")

        original = read_search_base(search_path)

        print("Consultas encontradas:", len(original))

        # 3. Criar índice atualizado
        print("\n[4/6] Indexando endereços no Elasticsearch...")

        client = get_client()

        try:
            if client.indices.exists(index=INDEX_NAME):
                client.indices.delete(index=INDEX_NAME)

            create_index(client, INDEX_NAME)
        finally:
            client.close()

        prepared_for_index = spark.read.parquet(
            str(prepared_path)
        )

        total_indexado = index_dataframe(
            prepared_for_index,
            INDEX_NAME,
        )

        print("Documentos no índice:", total_indexado)

        # 4. Executar matching
        print("\n[5/6] Buscando correspondências...")

        results = match_dataframe(
            original,
            INDEX_NAME,
        )

        # 5. Validar e exportar
        print("\n[6/6] Validando resultados...")

        report = validate_results(original, results)

        output_csv = REPORTS_DIR / "resultado_geocodificacao.csv"
        output_json = REPORTS_DIR / "relatorio_qualidade.json"

        results.to_csv(
            output_csv,
            index=False,
            encoding="utf-8-sig",
        )

        report["documentos_indexados"] = int(total_indexado)

        report["percentual_correspondencia_exata"] = round(
            (
                results["status_match"].eq("MATCH_EXATO").mean()
                * 100
            ),
            2,
        )

        with output_json.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=2,
                default=str,
            )

        print("\nPipeline concluída!")
        print("Resultado:", output_csv)
        print("Relatório:", output_json)
        print(json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        ))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
