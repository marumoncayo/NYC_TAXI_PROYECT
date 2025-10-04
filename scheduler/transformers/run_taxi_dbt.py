import subprocess
import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def run_dbt_transformations(*args, **kwargs):
    # Configurar variables de entorno
    os.environ['SNOW_ACCOUNT'] = get_secret_value('SNOW_ACCOUNT')
    os.environ['SNOW_USER'] = get_secret_value('SNOW_USER')
    os.environ['SNOW_PASSWORD'] = get_secret_value('SNOW_PASSWORD')
    os.environ['SNOW_ROLE'] = get_secret_value('SNOW_ROLE')
    os.environ['SNOW_WAREHOUSE'] = get_secret_value('SNOW_WAREHOUSE')
    os.environ['SNOW_DATABASE'] = get_secret_value('SNOW_DATABASE')
    
    # Ruta al proyecto dbt
    dbt_project_path = '/home/dbt/taxi_project'
    
    # Ejecutar dbt
    commands = [
        'dbt run --select silver',
        'dbt run --select gold',
        'dbt test'
    ]
    
    results = []
    for cmd in commands:
        print(f"Ejecutando: {cmd}")
        result = subprocess.run(
            cmd.split(),
            cwd=dbt_project_path,
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"ERROR: {result.stderr}")
            raise Exception(f"Falló: {cmd}")
        
        results.append({"command": cmd, "status": "success"})
    
    return results