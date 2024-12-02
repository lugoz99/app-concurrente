from fastapi import APIRouter, Depends, UploadFile, File, BackgroundTasks
import os
import tempfile
from database.mongo import Database
from services.genoma_service import GenomeProcessorService
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

router = APIRouter()


# Define the response model
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
                print(f"Archivo procesado en segundo plano: {temp_file_path}")
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
            message="Archivo recibido y procesado en segundo plano"
        )
    except Exception as e:
        return ProcessFileResponse(message=f"Error procesando archivo: {str(e)}")


@router.get("/create")
async def main(db: AsyncIOMotorDatabase = Depends(Database.get_db)):
    await db["books"].insert_one({"hello": "world"})
    return "Done"
