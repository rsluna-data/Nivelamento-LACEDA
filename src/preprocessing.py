
import re
import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def normalize_text(value):
    if value is None:
        return None

    value = str(value).strip().upper()

    value = unicodedata.normalize("NFKD", value)
    value = "".join(
        char for char in value
        if not unicodedata.combining(char)
    )

    value = re.sub(r"[^A-Z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()

    return value or None


def normalize_street(value):
    """
    Normaliza o logradouro e padroniza abreviações comuns.
    """
    value = normalize_text(value)

    if not value:
        return None

    # Padroniza tipos de logradouro.
    replacements = {
        r"\bR\b": "RUA",
        r"\bRUA\b": "RUA",
        r"\bAV\b": "AVENIDA",
        r"\bAVENIDA\b": "AVENIDA",
        r"\bROD\b": "RODOVIA",
        r"\bRODOVIA\b": "RODOVIA",
        r"\bTV\b": "TRAVESSA",
        r"\bTRAV\b": "TRAVESSA",
        r"\bTRAVESSA\b": "TRAVESSA",
        r"\bEST\b": "ESTRADA",
        r"\bESTR\b": "ESTRADA",
        r"\bESTRADA\b": "ESTRADA",
        r"\bPCA\b": "PRACA",
        r"\bPC\b": "PRACA",
        r"\bPRACA\b": "PRACA",
        r"\bAL\b": "ALAMEDA",
        r"\bALAMEDA\b": "ALAMEDA",
    }

    for pattern, replacement in replacements.items():
        value = re.sub(pattern, replacement, value)

    value = re.sub(r"\s+", " ", value).strip()

    return value or None

normalize_udf = F.udf(normalize_text, StringType())



def read_cnefe(spark, path, separator=";"):
    colunas_necessarias = [
        "COD_UNICO_ENDERECO",
        "COD_MUNICIPIO",
        "COD_SETOR",
        "CEP",
        "NOM_TIPO_SEGLOGR",
        "NOM_TITULO_SEGLOGR",
        "NOM_SEGLOGR",
        "NUM_ENDERECO",
        "DSC_MODIFICADOR",
        "DSC_LOCALIDADE",
    ]

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("sep", separator)
        .option("encoding", "UTF-8")
        .csv(path)
        .select(*colunas_necessarias)
    )



def prepare_cnefe(df: DataFrame) -> DataFrame:
    required = {
        "COD_UNICO_ENDERECO",
        "COD_MUNICIPIO",
        "COD_SETOR",
        "CEP",
        "NOM_TIPO_SEGLOGR",
        "NOM_TITULO_SEGLOGR",
        "NOM_SEGLOGR",
        "NUM_ENDERECO",
        "DSC_MODIFICADOR",
        "DSC_LOCALIDADE",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Colunas ausentes no CNEFE: {sorted(missing)}"
        )

    # Mantém os códigos geográficos como strings.
    
    # Renomeia todas as colunas para minúsculas
    # e mantém os valores como strings.
    df = df.select(
        *[
            F.col(column).cast("string").alias(column.lower())
            for column in df.columns
        ]
    )


    df = df.withColumn(
        "logradouro_original",
        F.concat_ws(
            " ",
            F.col("nom_tipo_seglogr"),
            F.col("nom_titulo_seglogr"),
            F.col("nom_seglogr"),
        )
    )

    normalize_street_udf = F.udf(
    normalize_street,
    StringType(),
)
    
    df = df.withColumn(
    "logradouro_normalizado",
    normalize_street_udf(
        F.col("logradouro_original")
    )
)
    
    df = df.withColumn(
    "localidade_normalizada",
    normalize_udf(F.col("dsc_localidade"))
)

    df = df.withColumn(
        "cep_normalizado",
        F.regexp_replace(
            F.coalesce(F.col("cep"), F.lit("")),
            r"[^0-9]",
            ""
        )
    )

    df = df.withColumn(
        "numero_normalizado",
        F.when(
            F.upper(F.trim(F.col("dsc_modificador"))) == "SN",
            F.lit("SN")
        ).otherwise(
            normalize_udf(F.col("num_endereco"))
        )
    )
    
    df = df.withColumn(
        "endereco_normalizado",
        normalize_udf(
            F.concat_ws(
                " ",
                F.col("logradouro_original"),
                F.col("num_endereco"),
                F.col("dsc_localidade"),
            )
        )
    )

    # Remove apenas linhas sem código de setor.
    # Não remove duplicatas automaticamente.
    df = df.filter(
        F.col("cod_setor").isNotNull()
        & (F.trim(F.col("cod_setor")) != "")
    )

    return df


def read_search_base(path):
    """
    A base de busca é separada por ponto e vírgula,
    conforme identificado na inspeção.
    """
    import pandas as pd

    df = pd.read_csv(
        path,
        sep=";",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )

    df.columns = [
        normalize_text(column).lower().replace(" ", "_")
        for column in df.columns
    ]

    expected = {
        "consulta_logradouro",
        "consulta_numero",
        "consulta_bairro",
        "consulta_cep",
        "consulta_complemento",
        "consulta_municipio",
    }

    missing = expected - set(df.columns)

    if missing:
        raise ValueError(
            "A base de busca não contém as colunas esperadas. "
            f"Ausentes: {sorted(missing)}. "
            f"Encontradas: {df.columns.tolist()}"
        )

    # Identificador técnico: preserva todas as linhas,
    # inclusive endereços repetidos.
    df.insert(
        0,
        "id_consulta",
        [str(i) for i in range(len(df))]
    )

    df["logradouro_normalizado"] = (
    df["consulta_logradouro"].map(normalize_street)
)

    df["cep_normalizado"] = (
        df["consulta_cep"]
        .str.replace(r"[^0-9]", "", regex=True)
    )

    df["numero_normalizado"] = (
        df["consulta_numero"].map(normalize_text)
    )
        
    df["bairro_normalizado"] = (
        df["consulta_bairro"].map(normalize_text)
    )
    
    df["municipio_consulta"] = (
        df["consulta_municipio"].astype(str).str.strip()
    )

    return df


def save_prepared_data(df: DataFrame, path: str):
    """
    Salva o DataFrame preparado em formato Parquet,
    reduzindo a quantidade de gravações simultâneas.
    """
    (
        df.coalesce(4)
        .write
        .mode("overwrite")
        .parquet(path)
    )

