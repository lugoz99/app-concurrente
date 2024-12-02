from fastapi import APIRouter, Depends, UploadFile, File, BackgroundTasks
import os
import tempfile
from database.mongo import Database
from services.genoma_service import GenomeProcessorService
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

router = APIRouter()


class ProcessFileResponse(BaseModel):
    message: str
    files_processed: int
    total_time: float


# Define the response model
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
                # Llamar a la función para procesar el archivo en paralelo
                column_positions = {}  # Definir las posiciones de columna aquí
                inserted_count, total_time, total_lines = (
                    await processor.process_file_parallel(
                        temp_file_path, column_positions
                    )
                )
                background_tasks.add_task(cleanup_temp_file)
                return inserted_count, total_time
            except Exception as e:
                print(f"Error procesando el archivo en segundo plano: {str(e)}")
                return 0

        # Función para eliminar el archivo temporal
        def cleanup_temp_file():
            try:
                os.unlink(temp_file_path)
            except Exception as e:
                print(f"Error eliminando archivo temporal: {e}")

        # Añadir tarea en segundo plano
        files_processed, t_time = await process_file_in_background()

        return ProcessFileResponse(
            message="Archivo recibido y procesado en segundo plano",
            files_processed=files_processed,
            total_time=t_time,
        )

    except Exception as e:
        return ProcessFileResponse(
            message=f"Error procesando archivo: {str(e)}", files_processed=0
        )


@router.get("/create")
async def main(db: AsyncIOMotorDatabase = Depends(Database.get_db)):
    await db["books"].insert_one({"hello": "world"})
    return "Done"
