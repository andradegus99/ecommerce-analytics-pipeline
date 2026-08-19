def get_surrogate_key_expr(columns: list[str]) -> str:
    """
    Retorna uma expressão SQL DuckDB para calcular uma surrogate key via MD5.
    Os valores de cada coluna são concatenados com '|' como separador.

    Exemplo de saída:
        md5(concat_ws('|', col1::VARCHAR, col2::VARCHAR, ...))
    """
    cols_sql = ", ".join([f"{col}::VARCHAR" for col in columns])
    return f"md5(concat_ws('|', {cols_sql}))"


def merge_into_table(source: str, target: str, keys: list[str], all_columns: list[str]) -> str:
    """
    Gera dinamicamente um statement MERGE INTO do DuckDB.

    - Cria a cláusula ON a partir das colunas-chave (keys).
    - No WHEN MATCHED, verifica se qualquer coluna não-chave é diferente (IS DISTINCT FROM).
    - No WHEN NOT MATCHED, insere todas as colunas.

    Args:
        source:      Nome da tabela/view de origem.
        target:      Nome da tabela de destino.
        keys:        Lista de colunas que compõem a chave de unicidade.
        all_columns: Lista com todas as colunas (chave + não-chave).

    Returns:
        String com o SQL do MERGE pronto para execução.
    """
    non_key_columns = [c for c in all_columns if c not in keys]

    # Cláusula ON
    on_clause = "\n        AND ".join(
        [f"target.{k} = source.{k}" for k in keys]
    )

    # Condição de diferença no WHEN MATCHED
    distinct_conditions = "\n            OR ".join(
        [f"target.{c} IS DISTINCT FROM source.{c}" for c in non_key_columns]
    )

    # SET do UPDATE
    update_set = ",\n            ".join(
        [f"{c} = source.{c}" for c in non_key_columns]
    )

    # INSERT
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join([f"source.{c}" for c in all_columns])

    query = f"""
        MERGE INTO {target} AS target
        USING {source} AS source
        ON {on_clause}

        WHEN MATCHED AND (
            {distinct_conditions}
        ) THEN UPDATE SET
            {update_set}

        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    return query