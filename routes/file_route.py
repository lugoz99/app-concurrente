from datetime import datetime
import time
from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks
import os
import tempfile
import logging

from services.genoma_service import process_parallel


router = APIRouter()
logging.basicConfig(level=logging.INFO)


def parallel_process_file(file_path):
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

        background_tasks.add_task(parallel_process_file, temp_file_path)
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
