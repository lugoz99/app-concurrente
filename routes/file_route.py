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
from database.mongo import Database
from schemas.statics_shcemas import PerformanceMetrics
from services.genoma_service import process_parallel,collection
from pymongo.collection import Collection
import hashlib

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
    page_size: int = Query(5000, ge=20, le=20000, description="Tamaño del lote"),
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

        duration = time.time() - start_time

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
#! modificar el concern
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


# Inicializar caché
cache = TTLCache(maxsize=1000, ttl=600)


def hash_query(query):
    """Genera un hash único para una consulta."""
    query_str = str(sorted(query.items()))
    return hashlib.md5(query_str.encode()).hexdigest()


def update_total(query):
    """Actualiza el conteo total de documentos en el caché."""
    try:
        query_hash = hash_query(query)  # Generar hash único para el query
        if query_hash in cache:
            return cache[query_hash]

        # Si no está en caché, calcular y almacenar el total
        total = collection.count_documents(query)
        cache[query_hash] = total
        return total
    except Exception as e:
        logging.error(f"Error actualizando el total en caché: {str(e)}", exc_info=True)
        return 0


def serialize_document(document):
    """Convierte un documento MongoDB para que sea serializable en JSON."""
    if isinstance(document, list):
        return [serialize_document(doc) for doc in document]
    if isinstance(document, dict):
        return {key: serialize_document(value) for key, value in document.items()}
    if isinstance(document, ObjectId):
        return str(document)
    return document


def execute_parallel_tasks(query, hint, chunk_size, start_after):
    try:
        # Añadir filtro por `_id` para avanzar correctamente
        if start_after:
            query["_id"] = {"$gt": ObjectId(start_after)}
        cursor = collection.find(query).hint(hint).limit(chunk_size)
        return list(cursor)
    except PyMongoError as e:
        logging.error(f"Error en consulta MongoDB: {str(e)}")
        return []


executor = ThreadPoolExecutor(max_workers=6)  # Global para mejor reutilización
def save_metrics_sync(metrics: PerformanceMetrics):
    Database.get_collection("metrics").insert_one(metrics.model_dump())


@router.get("/variants/bulk")
async def get_bulk_variants(
    field: str = Query(
        ..., description="Campo por el que se filtra", regex="CHROM|FILTER|INFO|FORMAT"
    ),
    value: str = Query(..., description="Valor para filtrar"),
    start_after: str = Query(None, description="ID del documento para continuar"),
    page_size: int = Query(20, ge=20, le=20000, description="Tamaño de la página"),
    chunk_size: int = Query(500, ge=100, le=5000, description="Tamaño de cada chunk"),
    workers: int = Query(6, ge=1, le=10, description="Número de hilos"),
):
    try:
        start_time = time.time()
        query = {field: value}
        hint = [(field, 1)]

        total_count = update_total(query)  # Usar la función mejorada de caché
        if total_count == 0:
            return {
                "variants": [],
                "total_count": 0,
                "documents_retrieved": 0,
                "chunks_processed": 0,
                "execution_time": "0 seconds",
                "message": "No se encontraron documentos.",
            }

        all_variants = []
        current_start_after = start_after
        remaining_documents = page_size
        chunks_processed = 0
        loop = asyncio.get_running_loop()  # Obtén el event loop actual
        while remaining_documents > 0:
            task_chunk_size = min(chunk_size, remaining_documents)
            chunk_results = await asyncio.gather(
                *[
                    loop.run_in_executor(
                        executor,
                        execute_parallel_tasks,
                        query.copy(),
                        hint,
                        task_chunk_size,
                        current_start_after,
                    )
                ]
            )

            for result in chunk_results:
                all_variants.extend(result)

            if all_variants:
                current_start_after = str(all_variants[-1]["_id"])
            remaining_documents -= task_chunk_size
            chunks_processed += 1

        retrieved_variants = serialize_document(
            list({str(doc["_id"]): doc for doc in all_variants}.values())[:page_size]
        )
        duration = time.time() - start_time

        metrics = PerformanceMetrics(
            field=field,
            value=value,
            type="paralela",
            num_workers=workers,
            chunk_size=chunk_size,
            query_time=duration,
            documents_retrieved=len(retrieved_variants),
            execution_method="paralela",
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        loop = asyncio.get_event_loop()
        loop.run_in_executor(executor, save_metrics_sync, metrics)

        response = {
            "variants": retrieved_variants,
            "total_documents": total_count,
            "documents_retrieved": len(retrieved_variants),
            "page_size": page_size,
            "chunks_processed": chunks_processed,
            "duration": f"{duration:.2f} seconds",
        }

        if len(retrieved_variants) == page_size:
            response["last_id"] = str(retrieved_variants[-1]["_id"])

        return response

    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"Error en get_bulk_variants: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Error procesando la consulta masiva."
        )

# secuencial
@router.get("/variants/bulk")
async def get_bulk_variants_seq(
    field: str = Query(
        ..., description="Campo por el que se filtra", regex="CHROM|FILTER|INFO|FORMAT"
    ),
    value: str = Query(..., description="Valor para filtrar"),
    page: int = Query(1, ge=1, description="Número de página (comienza en 1)"),
    page_size: int = Query(20, ge=1, le=20000, description="Tamaño de la página"),
):
    try:
        start_time = time.time()
        query = {field: value}

        # Calcular el total de documentos que cumplen el filtro
        total_count = collection.count_documents(query)

        # Si no hay documentos, retornar directamente
        if total_count == 0:
            return {
                "variants": [],
                "total_count": 0,
                "current_page": page,
                "page_size": page_size,
                "execution_time": "0 seconds",
                "message": "No se encontraron documentos.",
            }

        # Calcular el desplazamiento para la paginación
        skip = (page - 1) * page_size

        # Realizar la consulta a MongoDB con paginación
        variants = list(
            collection.find(query)
            .sort("_id", 1)  # Ordenar por ID ascendente
            .skip(skip)  # Desplazar documentos
            .limit(page_size)  # Limitar la cantidad de resultados
        )

        # Serializar los documentos
        retrieved_variants = serialize_document(variants)

        # Calcular la duración de la operación
        duration = time.time() - start_time
        metrics = PerformanceMetrics(
            field=field,
            value=value,
            type="secuencial",
            num_workers=None,  # No aplica
            chunk_size=None,  # No aplica
            query_time=duration,
            documents_retrieved=len(retrieved_variants),
            execution_method="secuencial",
            start_time=datetime.now(),
            end_time=datetime.now(),
        )
        # Preparar la respuesta
        loop = asyncio.get_event_loop()
        loop.run_in_executor(executor, save_metrics_sync, metrics)
        response = {
            "variants": retrieved_variants,
            "total_count": total_count,
            "current_page": page,
            "page_size": page_size,
            "execution_time": f"{duration:.2f} seconds",
        }

        return response

    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"Error en get_bulk_variants: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Error procesando la consulta paginada."
        )
