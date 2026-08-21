import duckdb
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
db_path = BASE_DIR / "data" / "warehouse.duckdb"
ingestion_script = BASE_DIR / "src" / "ingestion" / "load_csv_to_duckdb.py"

def main():
    print("=== SCRIPT DE TESTE ===")
    print("1. Dropando todas as tabelas do banco...\n")

    conn = duckdb.connect(str(db_path))

    tabelas = [row[0] for row in conn.sql("SHOW TABLES").fetchall()]

    if not tabelas:
        print("  Nenhuma tabela encontrada no banco.")
    else:
        for tabela in tabelas:
            conn.execute(f"DROP TABLE IF EXISTS {tabela}")
            print(f"  ✓ Tabela '{tabela}' dropada.")

    conn.close()

    print(f"\n2. Rodando {ingestion_script.name}...\n")
    print("=" * 50)

    resultado = subprocess.run(
        [sys.executable, str(ingestion_script), "--all"],
        cwd=str(BASE_DIR)
    )

    print("=" * 50)

    print(f"\n2. Rodando mais uma vez {ingestion_script.name} para testar merge e idempotência...\n")
    print("=" * 50)

    resultado = subprocess.run(
        [sys.executable, str(ingestion_script)],
        cwd=str(BASE_DIR)
    )

    print("=" * 50)

    if resultado.returncode == 0:
        print("\n✓ Script finalizado com sucesso!")
    else:
        print(f"\n✗ Script finalizou com erro (código {resultado.returncode}).")
        sys.exit(resultado.returncode)


if __name__ == "__main__":
    main()