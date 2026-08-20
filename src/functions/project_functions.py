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
    if non_key_columns:
        distinct_conditions = "\n            OR ".join(
            [f"target.{c} IS DISTINCT FROM source.{c}" for c in non_key_columns]
        )
        update_set = ",\n            ".join([f"{c} = source.{c}" for c in non_key_columns])
        when_matched_clause = f"""
        WHEN MATCHED AND (
            {distinct_conditions}
        ) THEN UPDATE SET
            {update_set}
        """
    else:
        # Se a tabela só possui chaves (sem colunas de atributos para atualizar)
        when_matched_clause = ""

    # INSERT
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join([f"source.{c}" for c in all_columns])

    query = f"""
        MERGE INTO {target} AS target
        USING {source} AS source
        ON {on_clause}
        {when_matched_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    return query


def calculate_merge_metrics(conn, source: str, target: str, keys: list[str], all_columns: list[str]) -> tuple[int, int, int]:
    """
    Calcula antes do MERGE quantos registros serão:
    - Inseridos (novos registros)
    - Atualizados (registros existentes com valores diferentes)
    - Inalterados (registros existentes idênticos)

    Returns:
        tuple[int, int, int]: (rows_inserted, rows_updated, rows_unchanged)
    """
    non_key_columns = [c for c in all_columns if c not in keys]

    join_conditions = " AND ".join([f"s.{k} = t.{k}" for k in keys])
    first_key = keys[0]

    # 1. Quantidade de novos registros a inserir (onde não houve match com target)
    query_insert = f"""
        SELECT COUNT(*)
        FROM {source} s
        LEFT JOIN {target} t ON {join_conditions}
        WHERE t.{first_key} IS NULL
    """
    rows_inserted = conn.execute(query_insert).fetchone()[0]

    # 2. Quantidade de registros a atualizar (match onde ao menos uma coluna é diferente)
    if non_key_columns:
        distinct_conditions = " OR ".join([f"t.{c} IS DISTINCT FROM s.{c}" for c in non_key_columns])
        query_update = f"""
            SELECT COUNT(*)
            FROM {source} s
            INNER JOIN {target} t ON {join_conditions}
            WHERE ({distinct_conditions})
        """
        rows_updated = conn.execute(query_update).fetchone()[0]
    else:
        rows_updated = 0

    # 3. Quantidade de registros inalterados (total no source - inserted - updated)
    total_source = conn.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
    rows_unchanged = total_source - (rows_inserted + rows_updated)

    return rows_inserted, rows_updated, rows_unchanged