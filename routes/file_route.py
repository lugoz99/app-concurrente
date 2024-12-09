import asyncio
import logging
import time
from typing import Optional
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Response,
    UploadFile,
    File,
    BackgroundTasks,
    logger,
)
import os
import tempfile

import orjson
from pydantic import BaseModel
from database.mongo import Database
from schemas.genoma_schema import ProcessFileResponse
from services.genoma_service import GenomeProcessorService
from motor.motor_asyncio import AsyncIOMotorDatabase

router = APIRouter()
logger = logging.getLogger("uvicorn")


# Define the response model
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    UploadFile,
    File,
    BackgroundTasks,
)
import asyncio
import logging
import os
import tempfile
import time
from motor.motor_asyncio import AsyncIOMotorDatabase
from database.mongo import Database
from schemas.genoma_schema import ProcessFileResponse
from services.genoma_service import GenomeProcessorService

router = APIRouter()
logger = logging.getLogger("uvicorn")


# Define el modelo de respuesta
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    UploadFile,
    File,
    BackgroundTasks,
)
import logging
import tempfile
import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorDatabase
from database.mongo import Database
from schemas.genoma_schema import ProcessFileResponse
from services.genoma_service import GenomeProcessorService

router = APIRouter()
logger = logging.getLogger("uvicorn")


class ProcessFileResponse(BaseModel):
    message: str


@router.post("/process_file", response_model=ProcessFileResponse)
async def process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(Database.get_db),
) -> ProcessFileResponse:

    processor = GenomeProcessorService(db)

    try:
        # Crear archivo temporal con un nombre más descriptivo
        with tempfile.NamedTemporaryFile(
            delete=False,
            suffix=f"_{file.filename}",
        ) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name

            # Validación básica de archivo
            if not content:
                print("Archivo vacío recibido")
                return ProcessFileResponse(message="El archivo está vacío")

            if not file.filename.lower().endswith(".vcf"):
                print(f"Archivo inválido: {file.filename}")
                return ProcessFileResponse(message="Por favor, suba un archivo VCF")

        # Función para procesar el archivo en segundo plano
        async def process_file_in_background():
            try:
                # Llama a tu función de procesamiento en paralelo
                await processor.process_file_parallel(temp_file_path)
            except Exception as e:
                print(f"Error procesando el archivo en segundo plano: {str(e)}")

        # Función para eliminar el archivo temporal
        def cleanup_temp_file():
            try:
                os.unlink(temp_file_path)
                print(f"Archivo temporal {temp_file_path} eliminado")
            except Exception as e:
                print(f"Error eliminando archivo temporal: {e}")

        # Añadir tareas en segundo plano
        background_tasks.add_task(process_file_in_background)
        background_tasks.add_task(cleanup_temp_file)

        return ProcessFileResponse(
            message="Archivo recibido y procesado en segundo plano",
        )
    except Exception as e:
        return ProcessFileResponse(message=f"Error procesando archivo: {str(e)}")


@router.get("/variants/bulk/")
async def get_bulk_variants(
    field: str = Query(
        ...,
        description="Campo por el que se va a filtrar (CHROM, FILTER, INFO, FORMAT)",
    ),
    value: str = Query(..., description="Valor del campo para filtrar"),
    page: int = Query(1, ge=1, description="Número de página para paginación"),
    page_size: int = Query(
        10000, ge=1000, le=20000, description="Número de documentos por página"
    ),
    chunk_size: int = Query(
        10000, ge=1000, le=20000, description="Tamaño de cada chunk de datos"
    ),
    max_chunks: int = Query(
        20, ge=1, le=40, description="Número máximo de chunks a recuperar"
    ),
    db: AsyncIOMotorDatabase = Depends(Database.get_db),
):
    """
    Recupera grandes cantidades de variantes genómicas con paginación optimizada.
    """
    try:
        start_time = time.time()

        # Crear consulta
        query = {field: value}

        # Contar total de documentos
        total_count = await db.get_collection("genomas_vcf").count_documents(query)
        if total_count == 0:
            return {
                "variants": [],
                "total_count": 0,
                "documents_retrieved": 0,
                "chunks_processed": 0,
                "execution_time": "0 seconds",
                "message": "No se encontraron documentos",
            }

        # Calcular documentos a saltar y límite por página
        skip_docs = (page - 1) * page_size
        if skip_docs >= total_count:
            return {
                "variants": [],
                "total_count": total_count,
                "documents_retrieved": 0,
                "message": f"No hay datos para la página {page}.",
            }

        # Calcular número de chunks necesarios
        chunks_needed = min(max_chunks, (page_size + chunk_size - 1) // chunk_size)
        partitions_per_chunk = min(20, max(1, chunk_size // 1000))

        # Crear tareas para cada chunk
        chunk_tasks = [
            execute_parallel_tasks(
                db=db,
                query=query,
                page_size=chunk_size,
                partitions=partitions_per_chunk,
                skip_docs=skip_docs + (chunk_index * chunk_size),
            )
            for chunk_index in range(chunks_needed)
        ]

        # Ejecutar tareas en paralelo
        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

        # Procesar resultados
        all_variants = []
        errors = []

        for idx, chunk_result in enumerate(chunk_results):
            if isinstance(chunk_result, Exception):
                errors.append(f"Error en chunk {idx}: {str(chunk_result)}")
            elif isinstance(chunk_result, list):
                all_variants.extend(chunk_result)

        # Limitar resultados a page_size
        retrieved_variants = all_variants[:page_size]
        execution_time = time.time() - start_time

        # Preparar respuesta
        response = {
            "variants": retrieved_variants,
            "total_count": total_count,
            "documents_retrieved": len(retrieved_variants),
            "current_page": page,
            "page_size": page_size,
            "chunks_processed": len(chunk_tasks),
            "execution_time": f"{execution_time:.2f} seconds",
        }

        if errors:
            response["errors"] = errors

        if total_count > skip_docs + len(retrieved_variants):
            response["next_page"] = page + 1

        if skip_docs > 0:
            response["previous_page"] = page - 1

        return response

    except Exception as e:
        logger.error(f"Error en get_bulk_variants: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error procesando la consulta masiva: {str(e)}"
        )


async def fetch_documents(db, query, skip=0, limit=100):
    try:
        collection = db.get_collection("genomas_vcf")
        cursor = collection.find(query).skip(skip).limit(limit)
        cursor.batch_size(1000)
        documents = await cursor.to_list(length=limit)

        return [{**doc, "_id": str(doc["_id"])} for doc in documents]
    except Exception as e:
        logger.error(f"Error fetching documents: {e}", exc_info=True)
        raise  # Propagar el error en lugar de retornar lista vacía


async def execute_parallel_tasks(db, query, page_size, partitions, skip_docs=0):
    partition_size = page_size // partitions
    tasks = [
        fetch_documents(
            db=db,
            query=query,
            skip=skip_docs + (i * partition_size),
            limit=partition_size,
        )
        for i in range(partitions)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filtrar y aplanar la lista de resultados
    return [doc for sublist in results if isinstance(sublist, list) for doc in sublist]
