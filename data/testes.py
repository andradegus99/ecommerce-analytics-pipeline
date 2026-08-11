import duckdb

conn = duckdb.connect("data/warehouse.duckdb")

print("--- Tabelas existentes no banco ---")
conn.sql("SHOW TABLES").show()

# print("\n--- Primeiras 5 linhas da tabela raw_customers ---")
# conn.sql("SELECT * FROM raw_customers LIMIT 5").show()

conn.close()