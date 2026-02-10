import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import time

# --- CONFIGURACIÓN ---
# Documentación API: https://www.coingecko.com/en/api/documentation
# Consultamos Bitcoin, Ethereum y Solana
URL_API = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,solana",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": "false"
}
# Nombre de la base de datos local (se creará sola)
DB_NAME = "crypto_market.db"

def extract_data():
    """
    Fase de Extracción: Conecta con la API y descarga los datos crudos.
    """
    try:
        print(f"[{datetime.now()}] Iniciando extracción...")
        response = requests.get(URL_API, params=PARAMS)
        response.raise_for_status() # Lanza error si la API falla (404, 500)
        data = response.json()
        print(f"--> Datos extraídos: {len(data)} registros encontrados.")
        return data
    except Exception as e:
        print(f"Error en la extracción: {e}")
        return None

def transform_data(data_json):
    """
    Fase de Transformación: Limpieza, selección de columnas y tipado.
    """
    if not data_json:
        return None
    
    print("Iniciando transformación...")
    
    # 1. Convertir JSON a DataFrame (Tabla)
    df = pd.DataFrame(data_json)
    
    # 2. Seleccionar solo las columnas que nos interesan para el negocio
    cols_to_keep = ['id', 'symbol', 'current_price', 'market_cap', 'total_volume', 'last_updated']
    df_clean = df[cols_to_keep]
    
    # 3. Añadir una columna de "Tiempo de Ingesta" (Metadata importante en Data Engineering)
    df_clean['ingestion_timestamp'] = datetime.now()
    
    # 4. Renombrar columnas para que sean más claras en SQL
    df_clean.rename(columns={'id': 'coin_id', 'current_price': 'price_usd'}, inplace=True)
    
    print("--> Datos transformados correctamente.")
    return df_clean

def load_data(df):
    """
    Fase de Carga: Guardar los datos en base de datos SQL.
    """
    if df is None:
        print("No hay datos para cargar.")
        return

    print("Iniciando carga en Base de Datos...")
    
    # Creamos el motor de conexión a SQLite
    engine = create_engine(f'sqlite:///{DB_NAME}')
    
    try:
        # if_exists='append': Añade datos nuevos sin borrar los viejos
        # index=False: No guardamos el índice numérico de Pandas
        df.to_sql('crypto_prices', con=engine, if_exists='append', index=False)
        print(f"--> Éxito: Datos cargados en la tabla 'crypto_prices' en {DB_NAME}")
    except Exception as e:
        print(f"Error cargando en BBDD: {e}")

# --- ORQUESTACIÓN (MAIN) ---
if __name__ == "__main__":
    # Ejecutamos el pipeline paso a paso
    raw_data = extract_data()
    clean_data = transform_data(raw_data)
    load_data(clean_data)
    
    print("\nPipeline finalizado.")