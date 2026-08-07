import duckdb
from pathlib import Path
import time

print("Iniciando processo de geração das tabelas raws.\n")
inicio = time.perf_counter()

raw_dir = Path("data/raw")
db_path = "data/warehouse.duckdb"

print("Conectando ao DuckDB...")
conn = duckdb.connect(db_path)

# Varre todos os arquivos .csv dentro da pasta data/raw
for csv_file in raw_dir.glob("*.csv"):
    
    clean_name = csv_file.stem.replace("olist_", "").replace("_dataset", "")
    table_name = f"raw_{clean_name}"
    
    # Converte o caminho para formato POSIX (compatível com Windows/Linux)
    csv_path = csv_file.as_posix()
    
    print(f"\nCarregando: {csv_file.name}")
    print(f" -> Tabela de destino: {table_name}")
    
    query = f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('{csv_path}')
    """
    
    try:
        conn.execute(query)
        print(f" ✓ Tabela '{table_name}' criada com sucesso!")
    except Exception as e:
        print(f" ✗ Erro ao carregar {csv_file.name}: {e}")

conn.close()

fim = time.perf_counter()
print(f"\nIngestão de todos os CSVs concluída em {fim - inicio:.2f} segundos!")