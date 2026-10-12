
def validate_results(original, results):
    required_columns = {
        "id_consulta",
        "codigo_setor",
        "status_match",
        "metodo_match",
        "candidatos_setor",
    }

    missing = required_columns - set(results.columns)

    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes: {sorted(missing)}"
        )

    if len(original) != len(results):
        raise ValueError(
            f"Quantidade de linhas mudou: "
            f"{len(original)} -> {len(results)}"
        )

    if original["id_consulta"].duplicated().any():
        raise ValueError("IDs duplicados na entrada.")

    if results["id_consulta"].duplicated().any():
        raise ValueError("IDs duplicados na saída.")

    if set(original["id_consulta"]) != set(
        results["id_consulta"]
    ):
        raise ValueError(
            "A entrada e a saída possuem IDs diferentes."
        )

    allowed_statuses = {
        "MATCH_EXATO",
        "AMBIGUO",
        "NAO_ENCONTRADO",
    }

    invalid = (
        set(results["status_match"].dropna().unique())
        - allowed_statuses
    )

    if invalid:
        raise ValueError(
            f"Status inválidos: {sorted(invalid)}"
        )

    unresolved = results["status_match"].isin(
        ["AMBIGUO", "NAO_ENCONTRADO"]
    )

    if results.loc[unresolved, "codigo_setor"].notna().any():
        raise ValueError(
            "Caso ambíguo ou não encontrado recebeu setor."
        )

    if results.loc[
        results["status_match"] == "MATCH_EXATO",
        "codigo_setor",
    ].isna().any():
        raise ValueError(
            "Correspondência exata sem código de setor."
        )

    counts = (
        results["status_match"]
        .value_counts(dropna=False)
        .to_dict()
    )

    return {
        "total_entrada": int(len(original)),
        "total_saida": int(len(results)),
        "preservou_todos_registros": True,
        "quantidade_por_status": counts,
    }
