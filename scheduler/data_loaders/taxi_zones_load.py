import pandas as pd
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value
import snowflake.connector

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=get_secret_value('SNOW_ACCOUNT'),
        user=get_secret_value('SNOW_USER'),
        password=get_secret_value('SNOW_PASSWORD'),
        role=get_secret_value('SNOW_ROLE'),
        warehouse=get_secret_value('SNOW_WAREHOUSE'),
        database=get_secret_value('SNOW_DATABASE'),
        schema=get_secret_value('SNOW_SCHEMA'),
        insecure_mode=True,
    )


@data_loader
def load_data(*args, **kwargs):
    schema = get_secret_value("SNOW_SCHEMA")
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    
    print(f"Descargando taxi zones desde {url}")
    
    df = pd.read_csv(url)
    df.columns = df.columns.str.upper()
    
    print(f"Registros cargados: {len(df)}")
    
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"""
        CREATE OR REPLACE TABLE {schema}.taxi_zones (
            LOCATIONID INT,
            BOROUGH VARCHAR(50),
            ZONE VARCHAR(100),
            SERVICE_ZONE VARCHAR(50)
        )
        """)
        print("Tabla creada")
        
        # Usar executemany con parámetros (más seguro y rápido)
        data_tuples = [
            (int(row['LOCATIONID']), 
             str(row['BOROUGH']), 
             str(row['ZONE']), 
             str(row['SERVICE_ZONE']))
            for _, row in df.iterrows()
        ]
        
        cursor.executemany(
            f"INSERT INTO {schema}.taxi_zones VALUES (%s, %s, %s, %s)",
            data_tuples
        )
        
        conn.commit()
        print(f"{len(df)} filas insertadas")
        
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.taxi_zones")
        count = cursor.fetchone()[0]
        print(f"Verificación: {count} filas")
        
        return [{"file": "taxi_zone_lookup.csv", "rows": count, "status": "ok"}]
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return [{"file": "taxi_zone_lookup.csv", "status": "error", "error": str(e)}]
    finally:
        cursor.close()
        conn.close()


@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output está vacío'