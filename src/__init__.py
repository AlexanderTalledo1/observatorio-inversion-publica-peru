
import os
import re
import csv
import tempfile
from pathlib import Path

import pandas as pd
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient, ContentSettings


# ============================================================
# CONFIGURACIÓN
# ============================================================

STORAGE_ACCOUNT = os.getenv(
    "AZURE_STORAGE_ACCOUNT",
    "stobservatorioperu01",
)

BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

# Prefijo donde se encuentra el archivo de origen en Bronze.
BRONZE_PREFIX = "mef/seguimiento_pi/"

# Ruta fija de salida para esta primera versión.
# La siguiente versión incorporará procesamiento por periodo.
SILVER_BLOB_NAME = (
    "mef/seguimiento_pi/2022-Seguimiento-PI.parquet"
)

CHUNK_SAMPLE_BYTES = 64 * 1024


# ============================================================
# CONEXIÓN A AZURE
# ============================================================

def create_blob_service() -> BlobServiceClient:
    """
    Se autentica usando la sesión iniciada con `az login`.
    No requiere guardar contraseñas ni claves de Storage.
    """
    credential = AzureCliCredential()

    account_url = (
        f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    )

    return BlobServiceClient(
        account_url=account_url,
        credential=credential,
    )


# ============================================================
# LOCALIZAR EL CSV DE BRONZE
# ============================================================

def find_latest_csv(container_client):
    """
    Busca CSV dentro del prefijo de Bronze y selecciona
    el archivo modificado más recientemente.
    """
    blobs = [
        blob
        for blob in container_client.list_blobs(
            name_starts_with=BRONZE_PREFIX
        )
        if blob.name.lower().endswith(".csv")
    ]

    if not blobs:
        raise FileNotFoundError(
            f"No se encontraron archivos CSV en "
            f"{BRONZE_CONTAINER}/{BRONZE_PREFIX}"
        )

    latest_blob = max(
        blobs,
        key=lambda blob: blob.last_modified,
    )

    print(f"Archivo de origen: {latest_blob.name}")
    print(f"Última modificación: {latest_blob.last_modified}")

    return latest_blob


# ============================================================
# DESCARGAR EL ARCHIVO
# ============================================================

def download_blob(blob_client, destination: str) -> None:
    """Descarga un blob a una ruta temporal local."""
    with open(destination, "wb") as file:
        stream = blob_client.download_blob()
        stream.readinto(file)

    if os.path.getsize(destination) == 0:
        raise ValueError("El archivo CSV descargado está vacío.")


# ============================================================
# DETECTAR DELIMITADOR Y LEER CSV
# ============================================================

def detect_delimiter(file_path: str) -> str:
    """
    Intenta detectar delimitadores comunes.
    Si la detección falla, utiliza coma.
    """
    with open(
        file_path,
        "r",
        encoding="utf-8-sig",
        errors="replace",
        newline="",
    ) as file:
        sample = file.read(CHUNK_SAMPLE_BYTES)

    try:
        dialect = csv.Sniffer().sniff(
            sample,
            delimiters=",;\t|",
        )
        return dialect.delimiter
    except csv.Error:
        print(
            "No se pudo detectar automáticamente el "
            "delimitador. Se utilizará coma."
        )
        return ","


def read_csv_safely(file_path: str) -> pd.DataFrame:
    """
    Lee el CSV intentando primero UTF-8 y después Latin-1.
    Conserva los valores como texto inicialmente cuando sea
    necesario para evitar inferencias prematuras de tipos.
    """
    delimiter = detect_delimiter(file_path)

    print(f"Delimitador detectado: {repr(delimiter)}")

    try:
        df = pd.read_csv(
            file_path,
            sep=delimiter,
            encoding="utf-8-sig",
            low_memory=False,
        )
    except UnicodeDecodeError:
        print("UTF-8 no funcionó; se intentará Latin-1.")
        df = pd.read_csv(
            file_path,
            sep=delimiter,
            encoding="latin-1",
            low_memory=False,
        )

    if df.empty and len(df.columns) <= 1:
        raise ValueError(
            "El CSV no contiene una estructura tabular "
            "reconocible. Revisa el delimitador y el archivo."
        )

    if len(df.columns) == 0:
        raise ValueError("El CSV no contiene columnas.")

    return df


# ============================================================
# LIMPIEZA Y VALIDACIONES
# ============================================================

def normalize_column_name(name: str) -> str:
    """
    Convierte nombres de columnas a minúsculas y snake_case.
    """
    name = str(name).strip().lower()

    # Reemplaza tildes y caracteres habituales.
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ü": "u",
        "ñ": "n",
    }

    for original, replacement in replacements.items():
        name = name.replace(original, replacement)

    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")

    return name or "columna_sin_nombre"


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres y evita colisiones de nombres
    después de la normalización.
    """
    new_names = []
    used_names = set()

    for original_name in df.columns:
        base_name = normalize_column_name(original_name)
        candidate = base_name
        suffix = 2

        while candidate in used_names:
            candidate = f"{base_name}_{suffix}"
            suffix += 1

        used_names.add(candidate)
        new_names.append(candidate)

    df.columns = new_names
    return df


def clean_dataframe(df: pd.DataFrame):
    """
    Limpieza conservadora:
    - normaliza nombres de columnas;
    - elimina espacios externos en textos;
    - convierte cadenas vacías en valores nulos;
    - elimina filas exactamente duplicadas.
    """
    rows_before = len(df)

    df = normalize_column_names(df)

    # Evita modificar las columnas que no son de texto.
    text_columns = df.select_dtypes(
        include=["object", "string"]
    ).columns

    for column in text_columns:
        df[column] = df[column].map(
            lambda value: (
                value.strip()
                if isinstance(value, str)
                else value
            )
        )

        df[column] = df[column].replace(
            r"^\s*$",
            pd.NA,
            regex=True,
        )

    duplicate_count = int(df.duplicated().sum())

    if duplicate_count:
        df = df.drop_duplicates().reset_index(drop=True)

    metrics = {
        "rows_before": rows_before,
        "rows_after": len(df),
        "duplicates_removed": duplicate_count,
        "columns": len(df.columns),
        "null_cells": int(df.isna().sum().sum()),
    }

    return df, metrics


# ============================================================
# GENERAR PARQUET
# ============================================================

def write_parquet(df: pd.DataFrame, output_path: str) -> None:
    """Escribe Parquet con compresión Snappy."""
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )


# ============================================================
# SUBIR A SILVER
# ============================================================

def upload_to_silver(
    silver_container_client,
    local_path: str,
) -> None:
    """
    Publica el Parquet en la ruta de destino.
    overwrite=True permite repetir la ejecución sin crear
    copias adicionales en esa misma ruta.
    """
    blob_client = silver_container_client.get_blob_client(
        SILVER_BLOB_NAME
    )

    with open(local_path, "rb") as file:
        blob_client.upload_blob(
            file,
            overwrite=True,
            content_settings=ContentSettings(
                content_type="application/vnd.apache.parquet"
            ),
        )

    print(f"Archivo publicado en Silver: {SILVER_BLOB_NAME}")


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

def main():
    print("=" * 60)
    print("OBSERVATORIO DE INVERSIÓN PÚBLICA DEL PERÚ")
    print("Pipeline Bronze -> Silver")
    print("=" * 60)

    blob_service = None

    try:
        blob_service = create_blob_service()

        bronze = blob_service.get_container_client(
            BRONZE_CONTAINER
        )
        silver = blob_service.get_container_client(
            SILVER_CONTAINER
        )

        source_blob = find_latest_csv(bronze)
        source_client = bronze.get_blob_client(
            source_blob.name
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = str(
                Path(temp_dir) / "source.csv"
            )
            parquet_path = str(
                Path(temp_dir) / "output.parquet"
            )

            print("\n[1/4] Descargando archivo...")
            download_blob(source_client, csv_path)

            print(
                f"Tamaño CSV: "
                f"{os.path.getsize(csv_path):,} bytes"
            )

            print("\n[2/4] Leyendo y validando CSV...")
            df = read_csv_safely(csv_path)

            print(f"Filas leídas: {len(df):,}")
            print(f"Columnas detectadas: {len(df.columns)}")

            print("\nColumnas originales:")
            print(list(df.columns))

            print("\n[3/4] Limpiando datos...")
            df, metrics = clean_dataframe(df)

            print(f"Filas iniciales: {metrics['rows_before']:,}")
            print(f"Filas finales: {metrics['rows_after']:,}")
            print(
                "Duplicados eliminados: "
                f"{metrics['duplicates_removed']:,}"
            )
            print(f"Número de columnas: {metrics['columns']}")
            print(
                "Celdas nulas después de la limpieza: "
                f"{metrics['null_cells']:,}"
            )

            print("\nColumnas finales:")
            print(list(df.columns))

            print("\n[4/4] Generando y cargando Parquet...")
            write_parquet(df, parquet_path)

            upload_to_silver(
                silver,
                parquet_path,
            )

            print(
                f"Tamaño Parquet: "
                f"{os.path.getsize(parquet_path):,} bytes"
            )

            print("\nPIPELINE COMPLETADO")
            print(f"Filas finales: {len(df):,}")
            print(
                "Destino: "
                f"{SILVER_CONTAINER}/{SILVER_BLOB_NAME}"
            )

    finally:
        if blob_service is not None:
            blob_service.close()


if __name__ == "__main__":
    main()
