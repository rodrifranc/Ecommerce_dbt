import os
import time
import httpx
import subprocess
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# 1. Cargar las credenciales ocultas desde tu archivo .env
load_dotenv()

AIRBYTE_HOST = os.getenv("AIRBYTE_HOST", "localhost")
AIRBYTE_PORT = os.getenv("AIRBYTE_PORT", "8000")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
AIRBYTE_USERNAME = os.getenv("AIRBYTE_USERNAME", "airbyte")
AIRBYTE_PASSWORD = os.getenv("AIRBYTE_PASSWORD", "password")

# 2. Tarea de Extracción y Carga (Airbyte)
@task(name="Extract and Load", retries=2, retry_delay_seconds=60)
def extract_and_load():
    logger = get_run_logger()
    base_url = f"http://{AIRBYTE_HOST}:{AIRBYTE_PORT}/api/v1"
    
    logger.info(f"Iniciando extracción con Airbyte (Connection ID: {AIRBYTE_CONNECTION_ID})")
    
    with httpx.Client(timeout=30.0, auth=(AIRBYTE_USERNAME, AIRBYTE_PASSWORD)) as client:
        response = client.post(
            f"{base_url}/connections/sync",
            json={"connectionId": AIRBYTE_CONNECTION_ID}
        )
        
        if response.status_code == 409:
            logger.warning("Ya hay un sync en curso en Airbyte. Esperando...")
            return None
        else:
            response.raise_for_status()
            job_id = response.json()["job"]["id"]
            
        logger.info(f"Job de Airbyte iniciado con ID: {job_id}. Esperando a que termine...")
        
        while True:
            status_resp = client.post(f"{base_url}/jobs/get", json={"id": job_id})
            status = status_resp.json()["job"]["status"]
            
            if status == "succeeded":
                logger.info("¡Extracción de Airbyte completada con éxito!")
                return job_id
            elif status in ("failed", "cancelled"):
                raise RuntimeError(f"El Job de Airbyte falló con estado: {status}")
            
            logger.info("Airbyte trabajando... verificando de nuevo en 10 segundos.")
            time.sleep(10)

# 3. Tarea de Transformación (dbt) con Subprocess (Método más estable)
@task(name="Transform with dbt")
def transform():
    logger = get_run_logger()
    logger.info("Iniciando transformaciones con dbt...")
    
    # Ejecutamos los comandos directamente en la terminal usando subprocess
    subprocess.run("dbt deps", shell=True, check=True)
    subprocess.run("dbt run", shell=True, check=True)
    
    logger.info("¡Transformaciones de dbt completadas con éxito!")

# 4. El Flow: Director de Orquesta
@flow(name="Ecommerce ELT Pipeline")
def ecommerce_pipeline():
    logger = get_run_logger()
    logger.info("Iniciando pipeline ELT de Maven Fuzzy Factory...")
    
    extract_and_load()
    transform()
    
    logger.info("¡Pipeline ELT completado de extremo a extremo!")

if __name__ == "__main__":
    ecommerce_pipeline()