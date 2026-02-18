import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import time
from datetime import datetime

# Leemos la URL de conexión desde las variables de entorno de Docker
# Si no existe (ej. pruebas locales), usa un valor por defecto
db_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/crypto_dw')

# Esperar a que Postgres arranque (retry logic básico)
def get_engine():
    retries = 5
    while retries > 0:
        try:
            engine = create_engine(db_url)
            engine.connect()
            print("--> Conexión a Postgres exitosa.")
            return engine
        except Exception as e:
            print("Base de datos no lista, reintentando en 5s...")
            time.sleep(5)
            retries -= 1
    raise Exception("No se pudo conectar a la base de datos.")

def run_pipeline():
    engine = get_engine()
    
    # 1. EXTRACT
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": "bitcoin,ethereum,solana", "order": "market_cap_desc", "sparkline": "false"}
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
    except Exception as e:
        print(f"Error API: {e}")
        return

    df = pd.DataFrame(data)
    
    # 2. TRANSFORM & MODELING
    
    # Tabla Dimensión: dim_coin (Datos estáticos)
    # Eliminamos duplicados para tener lista única de monedas
    dim_coin = df[['id', 'symbol', 'name']].drop_duplicates()
    
    # Tabla Hechos: fact_market_history (Datos transaccionales)
    fact_market = df[['id', 'current_price', 'market_cap', 'total_volume', 'last_updated']].copy()
    fact_market['ingestion_timestamp'] = datetime.now()
    fact_market.rename(columns={'id': 'coin_id'}, inplace=True) # Foreign Key

    # 3. LOAD (Carga separada)
    
    # Cargar Dimensión (if_exists='replace' para actualizar info básica o 'append' con cuidado)
    # En producción real usaríamos "Upsert", aquí simplificamos con replace para dimensiones pequeñas
    dim_coin.to_sql('dim_coin', engine, if_exists='replace', index=False)
    
    # Cargar Hechos (Siempre append, es histórico)
    fact_market.to_sql('fact_market_history', engine, if_exists='append', index=False)
    
    print(f"--> Datos insertados a las {datetime.now()}")

if __name__ == "__main__":
    print("Iniciando Pipeline Dockerizado...")
    # Loop infinito para simular un servicio continuo (cada 60 segundos)
    while True:
        run_pipeline()
        print("Esperando 60 segundos para la siguiente ejecución...")
        time.sleep(60)