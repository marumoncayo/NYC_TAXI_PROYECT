import os
import pandas as pd
import pyarrow.parquet as pq
import tempfile
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value


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
    service_type = kwargs.get("service_type", "yellow")  # ahora carga Yellow
    chunk_size = kwargs.get("chunk_size", 100000)
    schema = get_secret_value("SNOW_SCHEMA")

    conn = get_snowflake_conn()
    results = []

    for year in range(2015, 2026):  # hasta 2025
        for month in range(1, 13):

            # Evitar error: no procesar más allá de agosto 2025
            if year == 2025 and month > 8:
                continue

            file_name = f"{service_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

            print(f"Descargando {url} en chunks de {chunk_size}")

            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                    r = requests.get(url, stream=True)
                    if r.status_code != 200:
                        print(f" No encontrado: {url}")
                        continue

                    for chunk in r.iter_content(chunk_size=8192):
                        tmp.write(chunk)

                    tmp_path = tmp.name

                print(f"Guardado temporal: {tmp_path}")

                parquet_file = pq.ParquetFile(tmp_path)

                for batch in parquet_file.iter_batches(batch_size=chunk_size):
                    df = batch.to_pandas()

                    if df.empty:
                        continue

                    # Metadata
                    df["_ingest_year"] = year
                    df["_ingest_month"] = month
                    df["_source_file"] = file_name

                    target_table = f"{service_type}_trips"
                    df.columns = [c.upper() for c in df.columns]

                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{target_table} (
                        {", ".join([f'"{col}" STRING' for col in df.columns])}
                    )
                    """
                    with conn.cursor() as cur:
                        cur.execute(create_sql)

                    success, nchunks, nrows, _ = write_pandas(
                        conn,
                        df,
                        target_table,
                        schema=schema,
                        quote_identifiers=True,
                        auto_create_table=True,
                    )

                    if success:
                        print(f"{nrows} filas insertadas en {schema}.{target_table} ({file_name})")
                        results.append({"file": file_name, "rows": nrows, "status": "ok"})
                    else:
                        print(f"Falló inserción: {file_name}")
                        results.append({"file": file_name, "status": "failed"})

                os.remove(tmp_path)

            except Exception as e:
                print(f"Error procesando {file_name}: {e}")
                results.append({"file": file_name, "status": "error", "error": str(e)})
                continue

    conn.close()
    return results


@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output está vacío'
    assert isinstance(output, list), 'El output debe ser una lista'
