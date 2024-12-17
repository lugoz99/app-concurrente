import os
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from fastapi import FastAPI, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import asyncio
from concurrent.futures import ProcessPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import pymongo

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Cargar variables de entorno
load_dotenv()
NUM_PROCESSES = max(1, os.cpu_count() - 1)
CHUNK_SIZE = 70000
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB_NAME")
BATCH_SIZE = 1000

# Conexión con MongoDB (Motor)
client = AsyncIOMotorClient(MONGO_URI)
db = client["PRUEBAS"]
collection = db["genomas"]

# Inicialización de FastAPI
app = FastAPI()


# Funciones principales
async def create_indices():
    """Crea índices en MongoDB para optimizar consultas."""
    indices = ["CHROM", "FILTER", "INFO", "FORMAT"]
    try:
        tasks = [collection.create_index([(field, 1)]) for field in indices]
        await asyncio.gather(*tasks)
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


async def bulk_insert_mongo(documents: List[Dict], retry_count: int = 3) -> int:
    """Inserta documentos en MongoDB usando motor."""
    if not documents:
        return 0
    operations = [
        document for document in documents
    ]  # No necesitamos InsertOne con Motor
    total_inserted = 0
    for attempt in range(retry_count):
        try:
            result = await collection.insert_many(
                operations
            )  # Usamos insert_many con Motor
            total_inserted += len(result.inserted_ids)
            return total_inserted
        except pymongo.errors.BulkWriteError as bwe:
            logging.error(f"BulkWriteError: {bwe.details}")
            if attempt < retry_count - 1:
                continue
            else:
                logging.error(
                    f"Error final en insert después de {retry_count} intentos"
                )
                return total_inserted
        except Exception as e:
            logging.error(f"Error no recuperable en insert: {e}")
            return total_inserted
    return total_inserted


def process_file_chunk(
    file_path: str, start_pos: int, end_pos: int, column_positions: Dict[str, int]
) -> List[Dict]:
    """Procesa un chunk de archivo VCF y devuelve una lista de documentos."""
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


async def process_parallel(file_path: str):
    """Procesa el archivo en paralelo y realiza inserciones en MongoDB con batches."""
    start_datetime = datetime.now()
    logging.info(f"Iniciando procesamiento de {file_path}")
    logging.info(f"Hora de inicio: {start_datetime}")
    sample_columns, column_positions = get_header(file_path)
    logging.info(f"Encontradas {len(sample_columns)} columnas de muestras")
    await create_indices()

    total_processed = 0
    batch_documents = []  # Lista para acumular documentos del batch
    loop = asyncio.get_event_loop()

    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        chunk_positions = get_positions(file_path, CHUNK_SIZE)
        futures = [
            loop.run_in_executor(
                executor,
                process_file_chunk,
                file_path,
                start_pos,
                end_pos,
                column_positions,
            )
            for start_pos, end_pos in chunk_positions
        ]
        for future in asyncio.as_completed(futures):
            try:
                chunk_documents = await future
                if chunk_documents:
                    batch_documents.extend(chunk_documents)  # Acumular documentos

                    # Cuando el batch alcanza el tamaño definido, se inserta
                    if len(batch_documents) >= BATCH_SIZE:
                        total_processed += await bulk_insert_mongo(batch_documents)
                        batch_documents.clear()  # Limpiar el batch después de la inserción
            except Exception as e:
                logging.error(f"Error procesando chunk: {e}")

    # Insertar cualquier documento restante que no haya sido insertado en el batch
    if batch_documents:
        total_processed += await bulk_insert_mongo(batch_documents)

    logging.info(f"Total de documentos procesados: {total_processed}")


def get_positions(file_path: str, chunk_size: int) -> List[Tuple[int, int]]:
    """Divide el archivo en posiciones de chunks."""
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


# Endpoints de FastAPI
@app.post("/upload-vcf/")
async def upload_vcf(file: UploadFile, background_tasks: BackgroundTasks):
    if not file.filename.endswith(".vcf"):
        raise HTTPException(
            status_code=400, detail="El archivo debe tener extensión .vcf"
        )
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".vcf") as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(await file.read())
        background_tasks.add_task(process_parallel, temp_file_path)
        return JSONResponse(
            {
                "message": f"Archivo {file.filename} recibido y en proceso.",
                "path": temp_file_path,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {e}")


@app.get("/")
async def root():
    return {"message": "Servidor activo. Usa /upload-vcf para subir archivos."}


# Inicializar el servidor
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
