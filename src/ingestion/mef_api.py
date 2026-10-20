
import os
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


# Configuración de Azure
STORAGE_ACCOUNT = os.getenv(
    "AZURE_STORAGE_ACCOUNT",
    "stobservatorioperu01"
)
CONTAINER_NAME = "bronze"

# Archivo CSV publicado por el MEF
CSV_URL = (
    "https://fs.datosabiertos.mef.gob.pe/"
    "datastorefiles/2025-Seguimiento-PI.csv"
)


def download_csv():
    """Descarga el CSV del MEF a un archivo temporal."""
    response = requests.get(
        CSV_URL,
        stream=True,
        timeout=(15, 180)
    )
    response.raise_for_status()

    temp = tempfile.NamedTemporaryFile(
        mode="wb",
        suffix=".csv",
        delete=False
    )
    temp_path = temp.name

    total_bytes = 0

    try:
        with temp:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if chunk:
                    temp.write(chunk)
                    total_bytes += len(chunk)
    finally:
        response.close()

    if total_bytes == 0:
        os.remove(temp_path)
        raise RuntimeError("El MEF devolvió un archivo vacío.")

    print(f"Descarga completada: {total_bytes:,} bytes")
    return temp_path


def upload_to_bronze(file_path):
    """Carga el CSV original en Azure Blob Storage."""
    credential = DefaultAzureCredential()

    account_url = (
        f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    )

    blob_service = BlobServiceClient(
        account_url=account_url,
        credential=credential
    )

    container = blob_service.get_container_client(
        CONTAINER_NAME
    )

    fecha = datetime.now(
        ZoneInfo("America/Lima")
    ).strftime("%Y-%m-%d")

    blob_name = (
        "mef/seguimiento_proyectos/"
        f"ingestion_date={fecha}/"
        "2025-Seguimiento-PI.csv"
    )

    blob = container.get_blob_client(blob_name)

    with open(file_path, "rb") as archivo:
        blob.upload_blob(
            archivo,
            overwrite=True,
            content_type="text/csv"
        )

    print("Carga completada correctamente.")
    print(f"Contenedor: {CONTAINER_NAME}")
    print(f"Archivo: {blob_name}")


def main():
    temp_path = None

    try:
        temp_path = download_csv()
        upload_to_bronze(temp_path)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


if __name__ == "__main__":
    main()
