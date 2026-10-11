
from pathlib import Path
import pandas as pd

PASTA = Path("data")

for arquivo in PASTA.iterdir():
    if not arquivo.is_file():
        continue

    extensao = arquivo.suffix.lower()

    if extensao == ".csv" and "cnfe" in arquivo.name.lower():
        print("Arquivo:", arquivo.name)
        print("Tamanho (MB):", round(
            arquivo.stat().st_size / 1024**2, 2
        ))

        df = pd.read_csv(
            arquivo,
            sep=None,
            engine="python",
            encoding="utf-8-sig",
            nrows=5,
            dtype=str
        )

        print("\nColunas:")
        print(df.columns.tolist())

        print("\nAmostra:")
        print(df.head(3).to_string(index=False))

    elif extensao == ".parquet" and "cnefe" in arquivo.name.lower():
        df = pd.read_parquet(arquivo)
        print("Arquivo:", arquivo.name)
        print("Colunas:", df.columns.tolist())
        print(df.head(3).to_string(index=False))

    elif extensao in [".xlsx", ".xls"] and "cnefe" in arquivo.name.lower():
        print("Arquivo Excel:", arquivo.name)
        df = pd.read_excel(arquivo, nrows=5, dtype=str)
        print("Colunas:", df.columns.tolist())
        print(df.head(3).to_string(index=False))
