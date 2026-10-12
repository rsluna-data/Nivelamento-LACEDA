
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession


BASE_DIR = Path("/app/data/raw")
CNEFE_PATH = str(BASE_DIR / "cnfe.csv")
BUSCA_PATH = BASE_DIR / "base_busca.csv"


def main():
    spark = (
        SparkSession.builder
        .appName("CNEFE-Data-Inspection")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        cnefe = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("sep", ";")
            .csv(CNEFE_PATH)
        )

        print("\n=== COLUNAS DO CNEFE ===")
        print(cnefe.columns)

        print("\n=== TIPOS DO CNEFE ===")
        cnefe.printSchema()

        print("\n=== AMOSTRA DO CNEFE ===")
        cnefe.show(5, truncate=False)

        print("\n=== BASE DE BUSCA ===")
        busca = pd.read_csv(BUSCA_PATH, dtype=str)
        print("Quantidade de registros:", len(busca))
        print("Colunas:", busca.columns.tolist())
        print(busca.head().to_string(index=False))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
