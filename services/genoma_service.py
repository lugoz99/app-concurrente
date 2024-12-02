import asyncio
import os
import time
import mmap
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import List, Dict

# Todo: Aumentar estos valores
BATCH_SIZE = 10000
CHUNK_SIZE = 1500
NUM_PROCESSES = os.cpu_count()  # Usamos el número de núcleos del sistema
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")


class GenomeProcessorService:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.collection = db["genomas_vcf"]

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
            print("Índices creados con éxito")
        except Exception as e:
            print(f"Error creando índices: {e}")

    def process_line(self, line: str, column_positions: Dict[str, int]) -> Dict:
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
            return None

    async def process_file_chunk(
        self,
        file_path: str,
        start_pos: int,
        chunk_size: int,
        column_positions: Dict[str, int],
    ) -> List[Dict]:
        """Procesa un fragmento del archivo y devuelve los documentos procesados."""
        batch = []

        with open(file_path, "r") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            mm.seek(start_pos)

            for _ in range(chunk_size):
                line = mm.readline().decode("utf-8")
                if not line:
                    break

                document = self.process_line(line, column_positions)
                if document:
                    batch.append(document)

            mm.close()

        return batch

    async def bulk_insert_mongo(self, data: List[Dict]):
        """Inserta múltiples documentos en MongoDB de manera asíncrona."""
        if not data:
            return 0

        operations = [doc for doc in data if doc is not None]

        if operations:
            try:
                # TODO: Probar con bulk_write
                result = await self.collection.insert_many(operations)
                return len(result.inserted_ids)
            except Exception as e:
                print(f"Error insertando documentos en MongoDB: {e}")
                return 0

    async def process_file_parallel(
        self, file_path: str, column_positions: Dict[str, int]
    ):
        """Función principal para procesar el archivo VCF de manera asíncrona y en paralelo usando asyncio."""
        start_time = time.time()
        start_datetime = datetime.now()

        # Crear índices antes de procesar el archivo
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

        # Usar asyncio.gather para ejecutar las tareas asíncronas en paralelo
        tasks = [
            self.process_file_chunk(file_path, start_pos, chunk_len, column_positions)
            for start_pos, chunk_len in chunk_positions
        ]

        # Obtener los resultados de todas las tareas
        results = await asyncio.gather(*tasks)

        # Insertar los documentos procesados en MongoDB
        all_documents = [doc for batch in results for doc in batch]
        inserted_count = await self.bulk_insert_mongo(all_documents)

        end_time = time.time()
        end_datetime = datetime.now()
        total_time = end_time - start_time

        return inserted_count, total_time, total_lines
