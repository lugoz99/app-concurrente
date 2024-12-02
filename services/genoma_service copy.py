import os
import time
from datetime import datetime
import logging
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import mmap
from typing import List, Dict, Tuple

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
BATCH_SIZE = 10000
NUM_PROCESSES = 8
CHUNK_SIZE = 10000


class GenomeProcessorService:
    def __init__(self, db):
        # Inicialización del cliente MongoDB con Motor (asíncrono)
        self.collection = db["genomas_vcf"]

    async def get_header_info(self, file_path: str) -> Tuple[List[str], Dict[str, int]]:
        """Extrae la información del encabezado del archivo VCF."""
        with open(file_path, "r") as f:
            for line in f:
                if line.startswith("#CHROM"):
                    headers = line.strip().split("\t")
                    sample_columns = headers[9:]
                    column_positions = {
                        name: idx + 9 for idx, name in enumerate(sample_columns)
                    }
                    return sample_columns, column_positions
        raise ValueError("No se encontró la línea de encabezado en el archivo VCF")

    async def process_line(self, line: str, column_positions: Dict[str, int]) -> Dict:
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

            # Procesar campos de muestra si existen
            if len(fields) > 8:
                for sample_name, position in column_positions.items():
                    if position < len(fields):
                        document[sample_name] = fields[position]

            return document
        except Exception as e:
            print(f"Error procesando línea: {e}")
            print(f"Contenido de la línea: {line}")
            return None

    async def bulk_insert_mongo(self, data: List[Dict]):
        """Inserta múltiples documentos en MongoDB de manera asíncrona."""
        if not data:
            return

        operations = [doc for doc in data if doc is not None]

        if operations:
            try:
                result = await self.collection.insert_many(operations)
                print(f"Insertados {result.inserted_ids} documentos")
            except Exception as e:
                print(f"Error durante la inserción masiva: {e}")

    async def process_file_chunk(
        self,
        file_path: str,
        start_pos: int,
        chunk_size: int,
        column_positions: Dict[str, int],
    ):
        """Procesa un fragmento del archivo."""
        batch = []

        with open(file_path, "r") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            mm.seek(start_pos)

            try:
                for _ in range(chunk_size):
                    line = mm.readline().decode("utf-8")
                    if not line:
                        break

                    document = await self.process_line(line, column_positions)
                    if document:
                        batch.append(document)

                    if len(batch) >= BATCH_SIZE:
                        await self.bulk_insert_mongo(batch)
                        batch = []

                # Insertar registros restantes
                if batch:
                    await self.bulk_insert_mongo(batch)

            except Exception as e:
                print(f"Error procesando fragmento: {e}")
            finally:
                mm.close()

    async def create_indices(self):
        """Crea índices para mejorar el rendimiento de las consultas."""
        try:
            await self.collection.create_index([("CHROM", 1)])
            await self.collection.create_index([("POS", 1)])
            await self.collection.create_index([("ID", 1)])
            await self.collection.create_index([("REF", 1)])
            await self.collection.create_index([("ALT", 1)])
            await self.collection.create_index([("FILTER", 1)])
            await self.collection.create_index([("INFO", 1)])
            await self.collection.create_index([("FORMAT", 1)])
            await self.collection.create_index([("output", 1)])

            print("Índices creados con éxito")
        except Exception as e:
            print(f"Error creando índices: {e}")

    async def process_file_parallel(self, file_path: str):
        """Función principal para procesar el archivo VCF de manera paralela."""
        start_time = time.time()
        start_datetime = datetime.now()

        logging.info(f"Starting processing of {file_path}")
        logging.info(f"Start time: {start_datetime}")

        # Obtener información del encabezado
        sample_columns, column_positions = await self.get_header_info(file_path)
        logging.info(f"Found {len(sample_columns)} sample columns")

        # Crear índices primero
        await self.create_indices()

        # Obtener las posiciones de los fragmentos a procesar
        chunk_positions = []
        total_lines = 0
        with open(file_path, "r") as f:
            while True:
                pos = f.tell()
                lines = [f.readline() for _ in range(CHUNK_SIZE)]
                if not lines[-1]:
                    break
                total_lines += len(lines)
                chunk_positions.append((pos, len(lines)))

        logging.info(f"Total lines to process: {total_lines}")
        logging.info(f"Number of chunks: {len(chunk_positions)}")

        # Procesar los fragmentos en paralelo
        futures = []
        for start_pos, chunk_len in chunk_positions:
            futures.append(
                asyncio.create_task(
                    self.process_file_chunk(
                        file_path, start_pos, chunk_len, column_positions
                    )
                )
            )

        await asyncio.gather(*futures)

        end_time = time.time()
        end_datetime = datetime.now()
        total_time = end_time - start_time

        logging.info(f"Processing completed at: {end_datetime}")
        logging.info(f"Total processing time: {total_time/60:.2f} minutes")
        logging.info(
            f"Average processing speed: {total_lines/total_time:.0f} lines/second"
        )
        logging.info(f"Number of processes used: {NUM_PROCESSES}")
        logging.info(f"Chunk size: {CHUNK_SIZE}")


# Ejemplo de uso
async def main():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DATABASE_NAME]
    processor = GenomeProcessorService(db)
    await processor.process_file_parallel("path/to/your/file.vcf")


if __name__ == "__main__":
    asyncio.run(main())
