

# import json
# import os
# from pathlib import Path

# from pyspark.sql import SparkSession

# from src.preprocessing import (
#     read_cnefe,
#     prepare_cnefe,
#     read_search_base,
#     save_prepared_data,
# )
# from src.indexing import (
#     get_client,
#     create_index,
#     index_dataframe,
#     INDEX_NAME,
# )
# from src.matching import match_dataframe
# from src.validation import validate_results


# BASE_DIR = Path("/app/data")
# RAW_DIR = BASE_DIR / "raw"
# PROCESSED_DIR = BASE_DIR / "processed"
# REPORTS_DIR = Path("/app/reports")


# def main():
#     cnefe_path = Path(
#         os.getenv(
#             "CNEFE_PATH",
#             str(RAW_DIR / "cnfe.csv"),
#         )
#     )

#     search_path = Path(
#         os.getenv(
#             "SEARCH_PATH",
#             str(RAW_DIR / "base_busca.csv"),
#         )
#     )

#     separator = os.getenv("CNEFE_SEPARATOR", ";")

#     if not cnefe_path.exists():
#         raise FileNotFoundError(
#             f"CNEFE não encontrado: {cnefe_path}"
#         )

#     if not search_path.exists():
#         raise FileNotFoundError(
#             f"Base de busca não encontrada: {search_path}"
#         )

#     PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
#     REPORTS_DIR.mkdir(parents=True, exist_ok=True)

#     spark = (
#         SparkSession.builder
#         .appName("CNEFE-Address-Linkage")
#         .master("local[*]")
#         .getOrCreate()
#     )

#     spark.sparkContext.setLogLevel("WARN")

#     try:
#         # 1. Ler e preparar o CNEFE
#         print("\n[1/6] Lendo CNEFE...")

#         cnefe = read_cnefe(
#             spark,
#             str(cnefe_path),
#             separator=separator,
#         )

#         print("Colunas encontradas:", len(cnefe.columns))

#         print("\n[2/6] Preparando os endereços...")

#         prepared = prepare_cnefe(cnefe)

#         prepared_path = PROCESSED_DIR / "cnefe_prepared"

#         save_prepared_data(
#             prepared,
#             str(prepared_path),
#         )

#         print("Base preparada salva em:", prepared_path)

#         # 2. Ler a base de busca
#         print("\n[3/6] Lendo base de busca...")

#         original = read_search_base(search_path)

#         print("Consultas encontradas:", len(original))

#         # 3. Criar índice atualizado
#         print("\n[4/6] Indexando endereços no Elasticsearch...")

#         client = get_client()

#         try:
#             if client.indices.exists(index=INDEX_NAME):
#                 client.indices.delete(index=INDEX_NAME)

#             create_index(client, INDEX_NAME)
#         finally:
#             client.close()

#         prepared_for_index = spark.read.parquet(
#             str(prepared_path)
#         )

#         total_indexado = index_dataframe(
#             prepared_for_index,
#             INDEX_NAME,
#         )

#         print("Documentos no índice:", total_indexado)

#         # 4. Executar matching
#         print("\n[5/6] Buscando correspondências...")

#         results = match_dataframe(
#             original,
#             INDEX_NAME,
#         )

#         # 5. Validar e exportar
#         print("\n[6/6] Validando resultados...")

#         report = validate_results(original, results)

#         output_csv = REPORTS_DIR / "resultado_geocodificacao.csv"
#         output_json = REPORTS_DIR / "relatorio_qualidade.json"

#         results.to_csv(
#             output_csv,
#             index=False,
#             encoding="utf-8-sig",
#         )

#         report["documentos_indexados"] = int(total_indexado)

#         report["percentual_correspondencia_exata"] = round(
#             (
#                 results["status_match"].eq("MATCH_EXATO").mean()
#                 * 100
#             ),
#             2,
#         )

#         with output_json.open(
#             "w",
#             encoding="utf-8",
#         ) as file:
#             json.dump(
#                 report,
#                 file,
#                 ensure_ascii=False,
#                 indent=2,
#                 default=str,
#             )

#         print("\nPipeline concluída!")
#         print("Resultado:", output_csv)
#         print("Relatório:", output_json)
#         print(json.dumps(
#             report,
#             ensure_ascii=False,
#             indent=2,
#             default=str,
#         ))

#     finally:
#         spark.stop()


# if __name__ == "__main__":
#     main()
import json
import logging
import os
import time
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


BASE_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/app/reports"))


def configure_logging():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = REPORTS_DIR / "logs.txt"
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logging.getLogger("laceda.pipeline"), log_path


def build_metrics(original, results, total_indexado, elapsed_seconds):
    total = int(len(results))
    status_counts = results["status_match"].fillna("STATUS_AUSENTE").value_counts().to_dict()
    method_counts = results["metodo_match"].fillna("METODO_AUSENTE").value_counts().to_dict()

    def percentages(counts):
        return {
            str(key): round((int(value) / total * 100), 2) if total else 0.0
            for key, value in counts.items()
        }

    candidates = results.get("candidatos_setor")
    if candidates is not None:
        candidate_counts = candidates.fillna("").astype(str).map(
            lambda value: len([item for item in value.split(";") if item.strip()])
        )
        with_candidates = int((candidate_counts > 0).sum())
        average_candidates = round(float(candidate_counts.mean()), 2) if total else 0.0
        max_candidates = int(candidate_counts.max()) if total else 0
    else:
        with_candidates, average_candidates, max_candidates = 0, 0.0, 0

    missing_fields = {}
    for column in (
        "consulta_logradouro", "consulta_numero", "consulta_cep",
        "consulta_bairro", "consulta_municipio"
    ):
        if column in original.columns:
            values = original[column].fillna("").astype(str).str.strip()
            missing_fields[column] = int(values.eq("").sum())

    exact = int(results["status_match"].eq("MATCH_EXATO").sum())
    ambiguous = int(results["status_match"].eq("AMBIGUO").sum())
    not_found = int(results["status_match"].eq("NAO_ENCONTRADO").sum())

    return {
        "total_entrada": int(len(original)),
        "total_saida": total,
        "documentos_indexados": int(total_indexado),
        "preservou_todos_registros": int(len(original)) == total,
        "quantidade_por_status": {str(k): int(v) for k, v in status_counts.items()},
        "percentual_por_status": percentages(status_counts),
        "quantidade_por_metodo": {str(k): int(v) for k, v in method_counts.items()},
        "percentual_por_metodo": percentages(method_counts),
        "matches_exatos": exact,
        "percentual_correspondencia_exata": round(exact / total * 100, 2) if total else 0.0,
        "ambiguos": ambiguous,
        "nao_encontrados": not_found,
        "consultas_com_candidatos": with_candidates,
        "media_candidatos_por_consulta": average_candidates,
        "maximo_candidatos_em_uma_consulta": max_candidates,
        "campos_de_entrada_vazios": missing_fields,
        "tempo_total_segundos": round(elapsed_seconds, 2),
    }


def main():
    logger, log_path = configure_logging()
    started_at = time.perf_counter()
    spark = None

    cnefe_path = Path(os.getenv("CNEFE_PATH", str(RAW_DIR / "cnfe.csv")))
    search_path = Path(os.getenv("SEARCH_PATH", str(RAW_DIR / "base_busca.csv")))
    separator = os.getenv("CNEFE_SEPARATOR", ";")

    logger.info("Início do pipeline LACEDA.")
    logger.info("CNEFE: %s", cnefe_path)
    logger.info("Base de busca: %s", search_path)
    logger.info("Separador CNEFE: %r", separator)
    logger.info("Arquivo de log: %s", log_path)

    try:
        if not cnefe_path.exists():
            raise FileNotFoundError(f"CNEFE não encontrado: {cnefe_path}")
        if not search_path.exists():
            raise FileNotFoundError(f"Base de busca não encontrada: {search_path}")

        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("[1/6] Lendo CNEFE.")
        spark = (
            SparkSession.builder
            .appName("CNEFE-Address-Linkage")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        cnefe = read_cnefe(spark, str(cnefe_path), separator=separator)
        logger.info("Colunas selecionadas do CNEFE: %d", len(cnefe.columns))

        logger.info("[2/6] Preparando endereços do CNEFE.")
        prepared = prepare_cnefe(cnefe)
        prepared_path = PROCESSED_DIR / "cnefe_prepared"
        save_prepared_data(prepared, str(prepared_path))
        logger.info("Base preparada salva em: %s", prepared_path)

        logger.info("[3/6] Lendo base de busca.")
        original = read_search_base(search_path)
        logger.info("Consultas encontradas: %d", len(original))

        logger.info("[4/6] Recriando índice Elasticsearch: %s", INDEX_NAME)
        client = get_client()
        try:
            if client.indices.exists(index=INDEX_NAME):
                client.indices.delete(index=INDEX_NAME)
                logger.info("Índice anterior removido.")
            create_index(client, INDEX_NAME)
        finally:
            client.close()

        prepared_for_index = spark.read.parquet(str(prepared_path))
        total_indexado = index_dataframe(prepared_for_index, INDEX_NAME)
        logger.info("Documentos indexados no Elasticsearch: %d", total_indexado)

        logger.info("[5/6] Executando matching de endereços.")
        results = match_dataframe(original, INDEX_NAME)

        logger.info("[6/6] Validando e exportando resultados.")
        quality_report = validate_results(original, results)
        elapsed = time.perf_counter() - started_at
        metrics = build_metrics(original, results, total_indexado, elapsed)
        report = {**quality_report, **metrics}

        output_csv = REPORTS_DIR / "resultado_geocodificacao.csv"
        output_json = REPORTS_DIR / "relatorio_qualidade.json"
        metrics_json = REPORTS_DIR / "metricas_pipeline.json"

        results.to_csv(output_csv, index=False, encoding="utf-8-sig")
        with output_json.open("w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2, default=str)
        with metrics_json.open("w", encoding="utf-8") as file:
            json.dump(metrics, file, ensure_ascii=False, indent=2, default=str)

        logger.info("Pipeline concluído com sucesso em %.2f segundos.", elapsed)
        logger.info("CSV de resultado: %s", output_csv)
        logger.info("Relatório de qualidade: %s", output_json)
        logger.info("Métricas: %s", metrics_json)
        logger.info("Resumo das métricas: %s", json.dumps(metrics, ensure_ascii=False))
        return report

    except Exception:
        logger.exception("Pipeline interrompido por erro.")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Sessão Spark encerrada.")
        logger.info("Fim da execução.")


if __name__ == "__main__":
    main()
