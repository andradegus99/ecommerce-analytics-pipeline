import duckdb
from pathlib import Path
import time
import sys
from datetime import datetime
import re

# Adiciona a pasta 'src/functions' ao caminho de busca do Python
FUNCTIONS_DIR = Path(__file__).resolve().parent.parent / "functions"
sys.path.append(str(FUNCTIONS_DIR))

from project_functions import get_surrogate_key_expr, merge_into_table, calculate_merge_metrics
from table_config import TABLE_CONFIG

print("Iniciando processo de geração das tabelas RAW.\n")
inicio = time.perf_counter()

BASE_DIR = Path(__file__).resolve().parent.parent.parent

raw_dir = BASE_DIR / "data" / "raw"
db_path = BASE_DIR / "data" / "warehouse.duckdb"

if not raw_dir.exists():
    print(f"Erro: A pasta '{raw_dir}' não foi encontrada!")
    sys.exit(1)

# Varre as partições YYYY/MM/DD em ordem cronológica e coleta todos os CSVs
csv_files: list[tuple[Path, str]] = []

for year_dir in sorted(raw_dir.glob("[0-9][0-9][0-9][0-9]")):
    for month_dir in sorted(year_dir.glob("[0-9][0-9]")):
        for day_dir in sorted(month_dir.glob("[0-9][0-9]")):
            partition_date = f"{year_dir.name}-{month_dir.name}-{day_dir.name}"
            for csv_file in sorted(day_dir.glob("*.csv")):
                csv_files.append((csv_file, partition_date))

if not csv_files:
    print(f"Erro: Nenhum arquivo .csv encontrado nas partições YYYY/MM/DD dentro de '{raw_dir}'.")
    sys.exit(1)

print("Conectando ao DuckDB...")
conn = duckdb.connect(db_path)

tabelas_sucesso = []
tabelas_com_erro = []

# Tabela de log de ingestão com métricas de linhas
ingestion_log = """
    CREATE TABLE IF NOT EXISTS ingestion_log (
        file_name VARCHAR,
        table_name VARCHAR,
        partition_date VARCHAR,
        ingestion_date TIMESTAMP,
        flag_se_merged BOOLEAN,
        rows_inserted BIGINT,
        rows_updated BIGINT,
        rows_unchanged BIGINT,
        status VARCHAR
    );"""

try:
    conn.execute(ingestion_log)
except Exception as e:
    print(f"Erro ao criar tabela de log: {e}")
    sys.exit(1)

# Varre todos os CSVs das partições YYYY/MM/DD em ordem cronológica
for csv_file, partition_date in csv_files:

    clean_name = re.sub(r'olist_|_dataset|_\d{4}_\d{2}_\d{2}', '', csv_file.stem)
    table_name = f"raw_{clean_name}"

    csv_path = csv_file.as_posix()

    print(f"\n[{partition_date}] Carregando: {csv_file.name}")
    print(f" -> Tabela de destino: {table_name}")

    table_exists_query = f"""
        SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = '{table_name}'
    """
    table_exists = conn.execute(table_exists_query).fetchone()[0] > 0

    current_timestamp = datetime.now()
    config = TABLE_CONFIG.get(table_name)

    # Monta a TEMP VIEW (adiciona surrogate_key se necessário)
    if config and config.get("surrogate"):
        surrogate_expr = get_surrogate_key_expr(config["surrogate_columns"])
        query_temp_view = f"""CREATE OR REPLACE TEMP VIEW stg_data AS
            SELECT *, {surrogate_expr} AS surrogate_key
            FROM read_csv_auto('{csv_path}')"""
    else:
        query_temp_view = f"""CREATE OR REPLACE TEMP VIEW stg_data AS SELECT * FROM read_csv_auto('{csv_path}')"""

    conn.execute(query_temp_view)
    columns = [row[0] for row in conn.execute("DESCRIBE stg_data").fetchall()]

    if not table_exists:
        print(f"A tabela {table_name} ainda não existe, criando uma nova...")
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM stg_data
        """
        try:
            conn.execute(query)
            total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f" ✓ Tabela '{table_name}' criada com sucesso! ({total_rows:,} registros inseridos)")
            tabelas_sucesso.append(table_name)
            conn.execute(
                """INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (csv_file.name, table_name, partition_date, current_timestamp, False, total_rows, 0, 0, 'SUCESSO')
            )
        except Exception as e:
            print(f" ✗ Erro ao carregar {csv_file.name}: {e}")
            tabelas_com_erro.append(table_name)
            conn.execute(
                """INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (csv_file.name, table_name, partition_date, current_timestamp, False, 0, 0, 0, 'FALHOU')
            )
    else:
        if not config:
            print(f" ⚠ Tabela {table_name} não encontrada no TABLE_CONFIG. Pulando MERGE.")
            tabelas_com_erro.append(table_name)
            conn.execute(
                """INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (csv_file.name, table_name, partition_date, current_timestamp, True, 0, 0, 0, 'FALHOU')
            )
        else:
            print(f"Tabela {table_name} encontrada. Executando o MERGE...")
            try:
                # 1. Calcula as métricas de inserção, atualização e inalterados
                rows_inserted, rows_updated, rows_unchanged = calculate_merge_metrics(
                    conn=conn,
                    source="stg_data",
                    target=table_name,
                    keys=config["keys"],
                    all_columns=columns
                )

                # 2. Executa o MERGE
                merge_query = merge_into_table("stg_data", table_name, config["keys"], columns)
                conn.execute(merge_query)

                print(f" ✓ MERGE da tabela '{table_name}' feito com sucesso!")
                print(f"    ↳ Inseridos: {rows_inserted:,} | Atualizados: {rows_updated:,} | Inalterados: {rows_unchanged:,}")
                
                tabelas_sucesso.append(table_name)
                conn.execute(
                    """INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (csv_file.name, table_name, partition_date, current_timestamp, True, rows_inserted, rows_updated, rows_unchanged, 'SUCESSO')
                )
            except Exception as e:
                print(f" ✗ Erro ao executar o MERGE: {e}")
                tabelas_com_erro.append(table_name)
                conn.execute(
                    """INSERT INTO ingestion_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (csv_file.name, table_name, partition_date, current_timestamp, True, 0, 0, 0, 'FALHOU')
                )
                sys.exit(1)

conn.close()

fim = time.perf_counter()

print(f"\nFinalizada a ingestão das tabelas em {fim - inicio:.2f} segundos!")
print(f"Tabelas criadas ou editadas: {tabelas_sucesso}")

if tabelas_com_erro:
    raise RuntimeError(f"Falha na carga! As tabelas que deram erro são: {tabelas_com_erro}")