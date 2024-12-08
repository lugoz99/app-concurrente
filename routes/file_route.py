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
@router.post("/process_file", response_model=ProcessFileResponse)
async def process_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(Database.get_db),
) -> ProcessFileResponse:

    processor = GenomeProcessorService(db)

    try:
        # Crear un archivo temporal para almacenar el archivo subido
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_{file.filename}"
        ) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name

            # Validaciones básicas
            if not content:
                return ProcessFileResponse(
                    message="El archivo está vacío", files_processed=0
                )

            if not file.filename.lower().endswith(".vcf"):
                return ProcessFileResponse(
                    message="Por favor, suba un archivo VCF", files_processed=0
                )

        # Función para procesar el archivo en segundo plano
        async def process_file_in_background():
            try:
                column_positions = {}
                inserted_count, total_time, _ = await processor.process_file_parallel(
                    temp_file_path, column_positions
                )
                background_tasks.add_task(cleanup_temp_file)
                return inserted_count, total_time
            except Exception as e:
                logger.error(f"Error procesando el archivo en segundo plano: {str(e)}")
                return 0, 0

        # Función para limpiar el archivo temporal
        def cleanup_temp_file():
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                logger.error(f"Error eliminando archivo temporal: {e}")

        # Procesar en segundo plano
        files_processed, t_time = await process_file_in_background()

        return ProcessFileResponse(
            message="Archivo recibido y procesado en segundo plano",
            files_processed=files_processed,
            total_time=t_time,
        )

    except Exception as e:
        logger.error(f"Error procesando archivo: {str(e)}")
        return ProcessFileResponse(
            message=f"Error procesando archivo: {str(e)}", files_processed=0
        )


@router.get("/variants/bulk/")
async def get_bulk_variants(
    field: str = Query(
        ...,
        description="Campo por el que se va a filtrar (CHROM, FILTER, INFO, FORMAT)",
    ),
    value: str = Query(..., description="Valor del campo para filtrar"),
    chunk_size: int = Query(
        5000, ge=1000, le=10000, description="Tamaño de cada chunk de datos"
    ),
    max_chunks: int = Query(
        10, ge=1, le=20, description="Número máximo de chunks a recuperar"
    ),
    db: AsyncIOMotorDatabase = Depends(Database.get_db),
):
    """
    Recupera grandes cantidades de variantes genómicas de forma optimizada.
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
                "chunks_processed": 0,
                "execution_time": "0 seconds",
                "message": "No se encontraron documentos",
            }

        # Calcular número de chunks
        chunks_needed = min(max_chunks, (total_count + chunk_size - 1) // chunk_size)
        partitions_per_chunk = min(20, max(1, chunk_size // 1000))

        # Crear tareas para cada chunk
        chunk_tasks = [
            execute_parallel_tasks(
                db=db,
                query=query,
                page_size=chunk_size,
                partitions=partitions_per_chunk,
                skip_docs=chunk_index * chunk_size,
            )
            for chunk_index in range(chunks_needed)
        ]

        # Ejecutar tareas en paralelo (concurrencia)
        chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

        # Procesar resultados
        all_variants = []
        errors = []

        for idx, chunk_result in enumerate(chunk_results):
            if isinstance(chunk_result, Exception):
                errors.append(f"Error en chunk {idx}: {str(chunk_result)}")
            elif isinstance(chunk_result, list):
                all_variants.extend(chunk_result)

        execution_time = time.time() - start_time

        # Preparar respuesta
        response = {
            "variants": all_variants,
            "total_count": total_count,
            "chunks_processed": len(chunk_tasks),
            "documents_retrieved": len(all_variants),
            "execution_time": f"{execution_time:.2f} seconds",
            "chunk_size": chunk_size,
        }

        if errors:
            response["errors"] = errors

        if total_count > len(all_variants):
            response["warning"] = (
                f"Hay más documentos disponibles ({total_count - len(all_variants)}). "
                f"Considere aumentar los parámetros de chunk_size o max_chunks."
            )

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

        # Convertir ObjectId a cadena
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        return documents
    except Exception as e:
        logger.error(f"Error fetching documents: {e}")
        return []


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
