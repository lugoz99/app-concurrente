import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
from typing import Optional
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
import os
import tempfile
import logging
from bson.errors import InvalidId
from fastapi.responses import ORJSONResponse
from cachetools import TTLCache
import pymongo
from pymongo.errors import PyMongoError
from services.genoma_service import process_parallel,collection
from pymongo.collection import Collection


router = APIRouter()

# ************************** CARGAR ARCHIVOS**************************************
# !: "mejorar subidad de datos!"
def get_parallel(file_path):
    process_parallel(file_path)


@router.post("/process_file")
async def process_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """Procesa un archivo VCF en segundo plano"""
    start_time = time.time()
    try:
        with tempfile.NamedTemporaryFile(
            delete=False,
            prefix=f"vcf_{datetime.now().strftime('%Y%m%d_%H%M%S')}_",
            suffix=f"_{file.filename}",
        ) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name

            if not content:
                logging.warning("Archivo vacío recibido")
                return {
                    "error": "El archivo está vacío",
                    "process_time": round(time.time() - start_time, 3),
                }

            if not file.filename.lower().endswith(".vcf"):
                logging.warning(f"Archivo inválido: {file.filename}")
                return {
                    "error": "Por favor, suba un archivo VCF",
                    "process_time": round(time.time() - start_time, 3),
                }

        def cleanup_temp_file():
            try:
                os.unlink(temp_file_path)
                logging.info(f"Archivo temporal {temp_file_path} eliminado")
            except Exception as e:
                logging.error(f"Error eliminando archivo temporal: {e}")

        background_tasks.add_task(get_parallel, temp_file_path)
        background_tasks.add_task(cleanup_temp_file)

        return {
            "message": f"Archivo '{file.filename}' cargado. Procesamiento iniciado.",
            "temp_file": temp_file_path,
            "process_time": round(time.time() - start_time, 3),
        }
    except Exception as e:
        logging.error(f"Error procesando archivo: {str(e)}")
        return {
            "error": f"Error procesando archivo: {str(e)}",
            "process_time": round(time.time() - start_time, 3),
        }


# ******** GET ALL VARIANTS *****************************************


# Configura caché en memoria con un TTL de 4 días (345600 segundos)
total_documents_cache = TTLCache(maxsize=1, ttl=345600)


# Función para actualizar el total de documentos
def update_total_documents_cache():
    try:
        total_documents_cache["total"] = collection.count_documents({})
    except Exception as e:
        logging.error(
            f"Error actualizando total de documentos: {str(e)}", exc_info=True
        )


# Inicializar el caché al arrancar el servidor
update_total_documents_cache()


@router.get("/variants/all", response_class=ORJSONResponse)
def get_all_variants(
    start_id: Optional[str] = Query(None, description="ID inicial para rango"),
    page_size: int = Query(5000, ge=100, le=20000, description="Tamaño del lote"),
):
    """
    Recupera documentos con paginación, sin cargar todos los datos en memoria.
    """
    try:
        start_time = time.time()

        query = {}
        if start_id:
            try:
                start_id = ObjectId(start_id)
            except InvalidId:
                raise HTTPException(
                    status_code=400,
                    detail={"error": "ID inválido", "message": "start_id no es válido"},
                )
            query["_id"] = {"$gt": start_id}

        # Recuperar documentos utilizando un cursor
        results_cursor = (
            collection.find(query).sort("_id", pymongo.ASCENDING).limit(page_size)
        )
        results = [{**doc, "_id": str(doc["_id"])} for doc in results_cursor]

        # Recuperar el total de documentos desde el caché
        total_documents = total_documents_cache.get("total")
        if total_documents is None:
            update_total_documents_cache()
            total_documents = total_documents_cache.get("total")

        duration = round(time.time() - start_time, 5)

        return {
            "variants": results,
            "last_id": results[-1]["_id"] if results else None,
            "documents_retrieved": len(results),
            "total_documents": total_documents,
            "page_size": page_size,
            "duration_seconds": duration,
        }
    except Exception as e:
        logging.error(f"Error en get_all_variants: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "Error procesando la consulta", "message": str(e)},
        )


"***************** GET VARIANT BY COLUMN AN VALUE ************************"
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("MONGO_DB_NAME")

def get_mongo_collection() -> Collection:
    """Configura y retorna la colección de MongoDB."""
    client = pymongo.MongoClient(
        MONGO_URI,
        maxPoolSize=None,
        connectTimeoutMS=30000,
        socketTimeoutMS=None,
        connect=False,
    )
    return client[DATABASE_NAME]["genomas"]


collection = get_mongo_collection()



