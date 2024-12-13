import os
from multiprocessing import Value
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Tuple
import concurrent
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv
import pymongo

# Configuración de logging


# Cargar variables de entorno
load_dotenv()
NUM_PROCESSES = max(1, os.cpu_count() - 1)
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 10000))
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB_NAME")
BATCH_SIZE = 1000


def get_mongo_collection() -> MongoClient:
    """Configura y retorna la colección de MongoDB."""
    client = MongoClient(
        MONGO_URI,
        maxPoolSize=None,
        connectTimeoutMS=30000,
        socketTimeoutMS=None,
        connect=False,
    )
    return client[DATABASE_NAME]["genomas"]


collection = get_mongo_collection()


def create_indices():
    """Crea índices en MongoDB para optimizar consultas."""
    indices = [
        "CHROM",
        "FILTER",
        "INFO",
        "FORMAT",
    ]
    try:
        for field in indices:
            collection.create_index([(field, 1)])
        logging.info("Índices creados con éxito.")
    except Exception as e:
        logging.error(f"Error creando índices: {e}")


def get_header(file_path: str) -> Tuple[List[str], Dict[str, int]]:
    """Extrae información del encabezado del archivo VCF."""
    with open(file_path, "r") as f:
        for line in f:
            if line.startswith("#CHROM"):
                headers = line.strip().split("\t")
                column_positions = {
                    name: idx for idx, name in enumerate(headers[9:], start=9)
                }
                return headers[9:], column_positions
    raise ValueError("No se encontró la línea de encabezado en el archivo VCF.")


def process_line(line: str, column_positions: Dict[str, int]) -> Dict:
    """Procesa una línea del archivo VCF y retorna un documento."""
    if line.startswith("#"):
        return None

    fields = line.strip().split("\t")
    if len(fields) < 8:
        return None

    try:
        document = {
            "CHROM": fields[0],
            "POS": fields[1],
            "ID": fields[2] if fields[2] != "." else None,
            "REF": fields[3],
            "ALT": fields[4],
            "QUAL": fields[5],
            "FILTER": fields[6],
            "INFO": fields[7],
            "FORMAT": fields[8],
        }

        for sample_name, position in column_positions.items():
            if position < len(fields):
                document[sample_name] = fields[position]

        return document

    except Exception as e:
        logging.error(f"Error procesando línea: {e}")
        logging.error(f"Contenido de la línea: {line}")
        return None


def get_positions(file_path: str, chunk_size: int) -> List[Tuple[int, int]]:
   
    chunk_boundaries = []
    with open(file_path, "rb") as f:
        chunk_start = 0
        count = 0

        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                if pos > chunk_start:
                    chunk_boundaries.append((chunk_start, pos))
                break

            count += 1
            if count >= chunk_size:
                chunk_boundaries.append((chunk_start, pos))
                chunk_start = pos
                count = 0

    return chunk_boundaries


def get_lines_file(file_path: str) -> int:
    """Cuenta el número total de líneas en el archivo."""
    with open(file_path, "rb") as f:
        return sum(1 for _ in f)


def bulk_insert_mongo(documents: List[Dict], retry_count: int = 3) -> int:
   
    if not documents:
        return 0

    operations = [InsertOne(doc) for doc in documents]
    total_inserted = 0

    for attempt in range(retry_count):
        try:
            result = collection.bulk_write(operations, ordered=False)
            return result.inserted_count
        except pymongo.errors.BulkWriteError as bwe:
            inserted = bwe.details.get("nInserted", 0)
            total_inserted += inserted

            if attempt < retry_count - 1:
                write_errors = bwe.details.get("writeErrors", [])
                failed_indexes = {error["index"] for error in write_errors}
                operations = [
                    op for i, op in enumerate(operations) if i in failed_indexes
                ]
            else:
                logging.error(
                    f"Error final en bulk write después de {retry_count} intentos"
                )
                return total_inserted
        except Exception as e:
            logging.error(f"Error no recuperable en bulk write: {e}")
            return total_inserted

    return total_inserted


def process_file_chunk(
    file_path: str, start_pos: int, end_pos: int, column_positions: Dict[str, int]
) -> List[Dict]:
    documents = []

    with open(file_path, "rb") as f:
        f.seek(start_pos)
        while f.tell() < end_pos:
            try:
                line = f.readline().decode("utf-8")
                if not line:
                    break

                document = process_line(line, column_positions)
                if document:
                    documents.append(document)

            except Exception as e:
                logging.error(f"Error procesando línea: {e}")

    return documents


def process_parallel(file_path: str):
    start_datetime = datetime.now()
    logging.info(f"Iniciando procesamiento de {file_path}")
    logging.info(f"Hora de inicio: {start_datetime}")

    total_lines = get_lines_file(file_path)

    sample_columns, column_positions = get_header(file_path)
    logging.info(f"Encontradas {len(sample_columns)} columnas de muestras")

    create_indices()

    chunk_positions= get_positions(file_path, CHUNK_SIZE)

    total_processed = Value("i", 0)

    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        futures = []
        for start_pos, end_pos in chunk_positions:
            futures.append(
                executor.submit(
                    process_file_chunk, file_path, start_pos, end_pos, column_positions
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                chunk_documents = future.result()
                if chunk_documents:
                    inserted_count = bulk_insert_mongo(chunk_documents)
                    with total_processed.get_lock():
                        total_processed.value += inserted_count
            except Exception as e:
                logging.error(f"Error procesando chunk: {e}")

    with total_processed.get_lock():
        final_processed = total_processed.value

    logging.info(f"Total de documentos procesados: {final_processed} de {total_lines}")
