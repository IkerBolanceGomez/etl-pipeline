import pandas as pd
import sqlite3

# Conectar a la base de datos
conn = sqlite3.connect('crypto_market.db')

# Ejecutar una query SQL
print("--- DATOS ALMACENADOS ---")
df = pd.read_sql_query("SELECT * FROM crypto_prices ORDER BY ingestion_timestamp DESC LIMIT 6", conn)
print(df)

conn.close()