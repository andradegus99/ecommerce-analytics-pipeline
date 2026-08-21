import duckdb
import os
import pytest
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Adiciona caminhos do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
FUNCTIONS_DIR = BASE_DIR / "src" / "functions"
sys.path.append(str(FUNCTIONS_DIR))

from table_config import TABLE_CONFIG

# Caminhos exclusivos para o ambiente de testes
TEST_DB_PATH = BASE_DIR / "data" / "warehouse_test.duckdb"
INGESTION_SCRIPT = BASE_DIR / "src" / "ingestion" / "load_csv_to_duckdb.py"

# Define variável de ambiente apontando para o banco de teste e força encoding UTF-8 no subprocesso
TEST_ENV = {
    **os.environ,
    "DUCKDB_PATH": str(TEST_DB_PATH),
    "PYTHONIOENCODING": "utf-8"
}


@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """
    Fixture executada automaticamente antes de todos os testes:
    1. Remove qualquer banco de teste residual anterior.
    2. Executa a primeira carga de ingestão com --all no banco de teste isolado.
    3. Ao final de todos os testes (teardown), remove o banco de testes.
    """
    # 1. Limpeza inicial
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()

    # 2. Executa a ingestão completa (--all) apontando para o banco de teste
    result = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--all"],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        pytest.fail(f"Falha na carga inicial do banco de teste:\n{result.stderr}")

    yield

    # 3. Limpeza final
    if TEST_DB_PATH.exists():
        try:
            TEST_DB_PATH.unlink()
        except PermissionError:
            pass


@pytest.fixture(scope="function")
def db_conn():
    """Fixture que fornece uma conexão ao banco de testes isolado, fechando ao fim de cada teste."""
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    yield conn
    conn.close()


# =====================================================================
# 1. TESTES DE ESTRUTURA E QUALIDADE DAS TABELAS RAW NO BANCO DE TESTE
# =====================================================================

def test_pipeline_execution():
    """✓ Valida que o banco de teste foi criado com sucesso."""
    assert TEST_DB_PATH.exists(), f"O banco de teste '{TEST_DB_PATH}' não foi criado!"


def test_raw_tables_exist(db_conn):
    """✓ Testa se todas as tabelas RAW configuradas existem no banco de teste."""
    tables_in_db = [row[0] for row in db_conn.sql("SHOW TABLES").fetchall()]
    
    for table_name in TABLE_CONFIG.keys():
        assert table_name in tables_in_db, f"Tabela '{table_name}' não foi encontrada no banco de teste!"


@pytest.mark.parametrize("table_name", list(TABLE_CONFIG.keys()))
def test_minimum_rows(db_conn, table_name):
    """✓ Testa se as tabelas possuem uma quantidade mínima de registros (> 0)."""
    count = db_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    assert count > 0, f"A tabela '{table_name}' está vazia!"


@pytest.mark.parametrize("table_name,config", TABLE_CONFIG.items())
def test_keys_not_null(db_conn, table_name, config):
    """✓ Testa se as chaves primárias/surrogate possuem valores NULL."""
    keys = config["keys"]
    null_conditions = " OR ".join([f"{k} IS NULL" for k in keys])
    
    query = f"""
        SELECT COUNT(*) 
        FROM {table_name} 
        WHERE {null_conditions}
    """
    null_count = db_conn.execute(query).fetchone()[0]
    assert null_count == 0, f"A tabela '{table_name}' possui {null_count} registros com chave(s) nula(s)!"


@pytest.mark.parametrize("table_name,config", TABLE_CONFIG.items())
def test_keys_are_unique(db_conn, table_name, config):
    """✓ Testa se as chaves primárias/surrogate são únicas (sem duplicatas)."""
    if not config.get("unique_key", True):
        pytest.skip(f"Tabela '{table_name}' permite duplicatas na camada RAW por design.")

    keys = config["keys"]
    keys_str = ", ".join(keys)
    
    query = f"""
        SELECT {keys_str}, COUNT(*) AS total
        FROM {table_name}
        GROUP BY {keys_str}
        HAVING COUNT(*) > 1
    """
    duplicates = db_conn.execute(query).fetchall()
    assert len(duplicates) == 0, f"A tabela '{table_name}' possui {len(duplicates)} chaves duplicadas!"


def test_ingestion_log_status(db_conn):
    """✓ Testa se todos os arquivos registrados no log foram processados com status 'SUCESSO'."""
    failed_logs = db_conn.execute(
        "SELECT file_name, table_name, status FROM ingestion_log WHERE status != 'SUCESSO'"
    ).fetchall()
    
    assert len(failed_logs) == 0, f"Foram encontrados registros com falha no ingestion_log: {failed_logs}"


def test_ingestion_log_metrics_consistency(db_conn):
    """✓ Valida que todas as entradas de sucesso no ingestion_log possuem métricas consistentes (> 0)."""
    logs = db_conn.execute("""
        SELECT file_name, table_name, rows_inserted, rows_updated, rows_unchanged
        FROM ingestion_log
        WHERE status = 'SUCESSO'
    """).fetchall()

    assert len(logs) > 0, "Nenhum log de sucesso encontrado!"

    for file_name, table_name, ins, upd, unc in logs:
        assert ins >= 0, f"rows_inserted negativo ({ins}) em '{file_name}'!"
        assert upd >= 0, f"rows_updated negativo ({upd}) em '{file_name}'!"
        assert unc >= 0, f"rows_unchanged negativo ({unc}) em '{file_name}'!"
        
        total_accounted = ins + upd + unc
        assert total_accounted > 0, (
            f"A soma das métricas para '{file_name}' na tabela '{table_name}' resultou em 0 linhas processadas!"
        )



# =====================================================================
# 2. TESTES DE COMPORTAMENTO DO MERGE E IDEMPOTÊNCIA
# =====================================================================

def test_idempotency_second_run():
    """✓ Testa se a reexecução da mesma partição não gera alterações quando os dados permanecem iguais."""
    execution_start_time = datetime.now()

    # Executa o pipeline novamente para a partição mais recente
    result = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT)],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Falha na reexecução de ingestão:\n{result.stderr}"

    # Valida no log que os arquivos da reexecução resultaram em 0 inserções e 0 updates
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    
    second_run_logs = conn.execute("""
        SELECT file_name, table_name, rows_inserted, rows_updated, rows_unchanged
        FROM ingestion_log
        WHERE ingestion_date >= ?
    """, (execution_start_time,)).fetchall()
    conn.close()

    assert len(second_run_logs) > 0, "Nenhum log foi registrado durante a reexecução!"

    for file_name, table_name, inserted, updated, unchanged in second_run_logs:
        assert inserted == 0, f"Reprocessamento de '{file_name}' na tabela '{table_name}' gerou {inserted} INSERTs inesperados!"
        assert updated == 0, f"Reprocessamento de '{file_name}' na tabela '{table_name}' gerou {updated} UPDATEs inesperados!"
        assert unchanged > 0, f"Reprocessamento de '{file_name}' na tabela '{table_name}' não registrou linhas inalteradas (unchanged = {unchanged})!"
        assert (inserted + updated + unchanged) > 0, f"Nenhuma linha foi processada para '{file_name}' na tabela '{table_name}'!"



@pytest.fixture
def temp_test_partition():
    """
    Fixture que cria uma partição temporária '1999/01/01' com o CSV olist_customers_dataset.csv
    e garante que o diretório seja completamente removido ao término do teste.
    """
    test_date = "1999-01-01"
    test_dir = RAW_DIR / "1999" / "01" / "01"
    test_dir.mkdir(parents=True, exist_ok=True)

    csv_file = test_dir / "olist_customers_dataset.csv"

    yield {"date": test_date, "dir": test_dir, "csv": csv_file}

    if (RAW_DIR / "1999").exists():
        shutil.rmtree(RAW_DIR / "1999", ignore_errors=True)


def test_merge_inserts_new_record(temp_test_partition):
    """✓ Testa se um novo registro inexistente é inserido na tabela RAW via MERGE."""
    csv_file = temp_test_partition["csv"]
    date = temp_test_partition["date"]
    new_customer_id = "test_cust_new_99999"

    # 1. Cria CSV com um registro inédito
    csv_content = (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        f"{new_customer_id},unique_test_99999,01000,sao paulo,SP\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    # 2. Executa a ingestão para a partição de teste
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert res.returncode == 0, f"Falha na execução do pipeline de ingestão:\n{res.stderr}"

    # 3. Consulta a RAW e confirma que o novo registro foi inserido
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    row = conn.execute(
        f"SELECT customer_city, customer_state FROM raw_customers WHERE customer_id = '{new_customer_id}'"
    ).fetchone()
    conn.close()

    assert row is not None, f"O registro '{new_customer_id}' não foi inserido na tabela raw_customers!"
    assert row[0] == "sao paulo"
    assert row[1] == "SP"


def test_merge_updates_changed_record(temp_test_partition):
    """✓ Testa se um registro existente com alteração sofre UPDATE no banco."""
    csv_file = temp_test_partition["csv"]
    date = temp_test_partition["date"]

    # 1. Obtém um registro existente do banco
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    existing = conn.execute(
        "SELECT customer_id, customer_unique_id, customer_zip_code_prefix FROM raw_customers LIMIT 1"
    ).fetchone()
    conn.close()

    assert existing is not None, "Nenhum cliente encontrado na tabela raw_customers!"
    cust_id, unique_id, zip_code = existing
    nova_cidade = "CIDADE_TESTE_MODIFICADA"

    # 2. Cria CSV alterando a cidade do cliente existente
    csv_content = (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        f"{cust_id},{unique_id},{zip_code},{nova_cidade},SP\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    # 3. Executa a ingestão
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert res.returncode == 0, f"Falha na execução do pipeline de ingestão:\n{res.stderr}"

    # 4. Consulta a RAW e confirma que a cidade foi atualizada
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    cidade_atual = conn.execute(
        f"SELECT customer_city FROM raw_customers WHERE customer_id = '{cust_id}'"
    ).fetchone()[0]
    conn.close()

    assert cidade_atual == nova_cidade, f"Esperava '{nova_cidade}', mas encontrou '{cidade_atual}'!"


def test_merge_ignores_unchanged_record(temp_test_partition):
    """✓ Testa se um registro com dados idênticos é ignorado (sem novos inserts ou updates)."""
    csv_file = temp_test_partition["csv"]
    date = temp_test_partition["date"]

    # 1. Obtém um registro existente com todas as colunas
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    row = conn.execute(
        "SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state "
        "FROM raw_customers LIMIT 1"
    ).fetchone()
    conn.close()

    assert row is not None, "Nenhum cliente encontrado na tabela raw_customers!"

    # 2. Cria CSV com dados exatamente iguais
    csv_content = (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    execution_time = datetime.now()

    # 3. Executa a ingestão
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert res.returncode == 0, f"Falha na execução do pipeline de ingestão:\n{res.stderr}"

    # 4. Valida no ingestion_log que rows_inserted=0, rows_updated=0 e rows_unchanged=1
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    log = conn.execute("""
        SELECT rows_inserted, rows_updated, rows_unchanged
        FROM ingestion_log
        WHERE table_name = 'raw_customers' AND ingestion_date >= ?
        ORDER BY ingestion_date DESC LIMIT 1
    """, (execution_time,)).fetchone()
    conn.close()

    assert log is not None, "Nenhum log de execução foi encontrado no ingestion_log!"
    inserted, updated, unchanged = log
    assert inserted == 0, f"Esperava 0 inserções, mas foram registradas {inserted}!"
    assert updated == 0, f"Esperava 0 atualizações, mas foram registradas {updated}!"
    assert unchanged == 1, f"Esperava 1 inalterado, mas foram registrados {unchanged}!"


def test_merge_composite_key(temp_test_partition):
    """✓ Prova que o MERGE reconhece chaves compostas (order_id, order_item_id)."""
    test_dir = temp_test_partition["dir"]
    date = temp_test_partition["date"]
    csv_file = test_dir / "olist_order_items_dataset.csv"

    # 1. Pega um order_id existente com item_id = 1
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    existing_item = conn.execute("""
        SELECT order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
        FROM raw_order_items
        WHERE order_item_id = 1
        LIMIT 1
    """).fetchone()
    conn.close()

    assert existing_item is not None, "Nenhum item de pedido encontrado na tabela raw_order_items!"
    order_id, item_id, prod_id, seller_id, limit_date, price, freight = existing_item
    new_item_id = 999  # Novo item para o mesmo pedido existente

    # 2. Gera CSV contendo o mesmo order_id mas com order_item_id novo (999)
    csv_content = (
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        f"{order_id},{new_item_id},{prod_id},{seller_id},{limit_date},{price},{freight}\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    # 3. Executa a ingestão
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert res.returncode == 0, f"Falha na ingestão de chave composta:\n{res.stderr}"

    # 4. Valida se o banco agora possui AMBOS os itens para o mesmo order_id
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    items = conn.execute(
        f"SELECT order_item_id FROM raw_order_items WHERE order_id = '{order_id}' ORDER BY order_item_id"
    ).fetchall()
    conn.close()

    item_ids = [row[0] for row in items]
    assert 1 in item_ids, "O item original (order_item_id=1) desapareceu!"
    assert 999 in item_ids, "O novo item (order_item_id=999) não foi inserido!"


def test_ingestion_fails_on_schema_mismatch(temp_test_partition):
    """✓ Valida que o pipeline rejeita arquivos cujo schema/colunas não correspondem ao esperado na tabela RAW."""
    test_dir = temp_test_partition["dir"]
    date = temp_test_partition["date"]
    csv_file = test_dir / "olist_customers_dataset.csv"

    # 1. Cria um CSV válido sintaticamente, mas com colunas incompatíveis com raw_customers
    invalid_schema_content = (
        "wrong_column_a,wrong_column_b\n"
        "valor_123,valor_456\n"
    )
    csv_file.write_text(invalid_schema_content, encoding="utf-8")

    # 2. Executa a ingestão
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )

    # 3. Garante que o pipeline falhou (MERGE tenta acessar chaves inexistentes na view stg_data)
    assert res.returncode != 0, "O pipeline deveria falhar com returncode != 0 ao receber colunas incompatíveis!"



def test_backfill_historical_partition(temp_test_partition):
    """✓ Prova a capacidade de backfill e reprocessamento retroativo."""
    csv_file = temp_test_partition["csv"]
    date = temp_test_partition["date"]

    # 1. Pega um cliente existente
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    row = conn.execute(
        "SELECT customer_id, customer_unique_id, customer_zip_code_prefix "
        "FROM raw_customers LIMIT 1"
    ).fetchone()
    conn.close()

    assert row is not None, "Nenhum cliente encontrado na raw_customers!"
    cust_id, unique_id, zip_code = row
    cidade_backfill = "CAMPINAS_BACKFILL"

    # 2. Gera partição de backfill com a nova cidade
    csv_content = (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        f"{cust_id},{unique_id},{zip_code},{cidade_backfill},SP\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    # 3. Executa a ingestão simulando um backfill daquela data
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert res.returncode == 0, f"Falha no backfill:\n{res.stderr}"

    # 4. Confirma que a RAW agora reflete o valor corrigido do backfill
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    cidade_atual = conn.execute(
        f"SELECT customer_city FROM raw_customers WHERE customer_id = '{cust_id}'"
    ).fetchone()[0]
    conn.close()

    assert cidade_atual == cidade_backfill, (
        f"Esperava que a cidade fosse atualizada para '{cidade_backfill}', mas obteve '{cidade_atual}'"
    )


def test_merge_metrics_exact_sum(temp_test_partition):
    """✓ Prova que inserted + updated + unchanged é exatamente igual ao total de linhas do CSV de origem."""
    csv_file = temp_test_partition["csv"]
    date = temp_test_partition["date"]

    # 1. Pega 3 clientes existentes no banco
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    existing = conn.execute("""
        SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
        FROM raw_customers
        LIMIT 3
    """).fetchall()
    conn.close()

    assert len(existing) >= 3, "Menos de 3 clientes encontrados na raw_customers!"

    c1, c2, c3 = existing[0], existing[1], existing[2]

    # Monta o cenário exato:
    # - c1: inalterado (UNCHANGED 1)
    # - c2: inalterado (UNCHANGED 2)
    # - c3: alterado (UPDATE 1)
    # - novo: inédito (INSERT 1)
    # Total de linhas no CSV = 4
    csv_content = (
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state\n"
        f"{c1[0]},{c1[1]},{c1[2]},{c1[3]},{c1[4]}\n"
        f"{c2[0]},{c2[1]},{c2[2]},{c2[3]},{c2[4]}\n"
        f"{c3[0]},{c3[1]},{c3[2]},CIDADE_NOVA_METRICAS,XX\n"
        f"cust_metric_new_01,uniq_metric_01,01000,sao paulo,SP\n"
    )
    csv_file.write_text(csv_content, encoding="utf-8")

    execution_time = datetime.now()

    # 2. Executa a ingestão
    res = subprocess.run(
        [sys.executable, str(INGESTION_SCRIPT), "--date", date],
        cwd=str(BASE_DIR),
        env=TEST_ENV,
        capture_output=True,
        text=True
    )
    assert res.returncode == 0, f"Falha na ingestão: {res.stderr}"

    # 3. Consulta as métricas gravadas no log
    conn = duckdb.connect(str(TEST_DB_PATH), read_only=True)
    log = conn.execute("""
        SELECT rows_inserted, rows_updated, rows_unchanged
        FROM ingestion_log
        WHERE table_name = 'raw_customers' AND ingestion_date >= ?
        ORDER BY ingestion_date DESC LIMIT 1
    """, (execution_time,)).fetchone()
    conn.close()

    assert log is not None, "Nenhum log encontrado para a execução!"
    inserted, updated, unchanged = log

    # 4. Asserções rigorosas
    assert inserted == 1, f"Esperava 1 INSERT, obteve {inserted}"
    assert updated == 1, f"Esperava 1 UPDATE, obteve {updated}"
    assert unchanged == 2, f"Esperava 2 UNCHANGED, obteve {unchanged}"
    assert (inserted + updated + unchanged) == 4, (
        f"A soma das métricas ({inserted + updated + unchanged}) difere do total de linhas do CSV (4)!"
    )