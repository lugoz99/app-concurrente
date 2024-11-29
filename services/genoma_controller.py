import asyncio
import logging
from typing import List, Dict, Optional, Generator
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorCollection
from fastapi import HTTPException, status, UploadFile
from tenacity import retry, stop_after_attempt, wait_exponential
from schemas.genoma_schema import VCFEntryCreate

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class VCFValidationError(Exception):
    """Excepción personalizada para errores de validación VCF"""

    pass


class VCFController:

    def __init__(
        self,
        db: AsyncIOMotorCollection,
        max_retries: int = 3,
        batch_size: int = 750,  # Reducido de 1000
        chunk_size: int = 4096,  # Reducido de 8192
    ):
        """
        Inicializa el controlador VCF.

        Args:
            db: Colección de MongoDB
            batch_size: Tamaño del lote para inserciones
            chunk_size: Tamaño del chunk para lectura de archivos
            max_retries: Número máximo de reintentos para operaciones de DB
        """
        self.db = db
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self._setup_indexes()

    async def _setup_indexes(self):
        """Configura los índices necesarios en la base de datos"""
        indexes = [
            {"key": [("chrom", 1), ("pos", 1)], "name": "chrom_pos_idx"},
            {"key": [("filter", 1)], "name": "filter_idx"},
            {"key": [("info", 1)], "name": "info_idx"},
            {"key": [("format", 1)], "name": "format_idx"},
            {"key": [("created_at", 1)], "name": "created_at_idx"},
        ]

        try:
            await self.db.create_indexes(indexes)
            logger.info("Índices creados exitosamente")
        except Exception as e:
            logger.error(f"Error al crear índices: {e}")
            raise

    def _is_valid_vcf_file(self, file: UploadFile) -> bool:
        """
        Valida si el archivo es un VCF válido.

        Args:
            file: Archivo a validar

        Returns:
            bool: True si el archivo es válido
        """
        valid_extensions = {".vcf", ".txt"}
        file_ext = "." + file.filename.split(".")[-1].lower()

        return file.content_type == "text/plain" and file_ext in valid_extensions

    def _validate_vcf_line(self, columns: List[str]) -> None:
        """
        Valida una línea de datos VCF.

        Args:
            columns: Lista de columnas de datos VCF

        Raises:
            VCFValidationError: Si la línea no es válida
        """
        required_columns = 9  # Número mínimo de columnas requeridas

        if len(columns) < required_columns:
            raise VCFValidationError(
                f"Línea VCF inválida: se esperaban al menos {required_columns} columnas, "
                f"se encontraron {len(columns)}"
            )

        try:
            int(columns[1])  # Validar que pos sea un número
        except ValueError:
            raise VCFValidationError(f"Posición inválida: {columns[1]}")

    def parse_vcf_chunk(self, content: str) -> Generator[VCFEntryCreate, None, None]:
        """
        Parsea un chunk de contenido VCF.

        Args:
            content: Contenido VCF a parsear

        Yields:
            VCFEntryCreate: Entrada VCF parseada
        """
        samples_ch_present = False
        samples_cs_present = False

        for line in content.split("\n"):
            if not line.strip():
                continue

            if line.startswith("#"):
                if "output_CH" in line:
                    samples_ch_present = True
                if "output_CS" in line:
                    samples_cs_present = True
                continue

            try:
                columns = line.strip().split("\t")
                self._validate_vcf_line(columns)

                entry = VCFEntryCreate(
                    chrom=columns[0],
                    pos=int(columns[1]),
                    ref=columns[3],
                    alt=columns[4],
                    qual=columns[5],
                    filter=columns[6],
                    info=columns[7],
                    format=columns[8],
                    samples_CH=self._parse_samples(
                        "CH", columns[9:], samples_ch_present
                    ),
                    samples_CS=self._parse_samples(
                        "CS", columns[9:], samples_cs_present
                    ),
                    created_at=datetime.utcnow(),
                )
                yield entry

            except Exception as e:
                logger.error(f"Error parsing VCF line: {line[:100]}... Error: {str(e)}")
                continue

    def _parse_samples(
        self, prefix: str, columns: List[str], is_present: bool
    ) -> Dict[str, str]:
        """
        Parsea las columnas de muestras.

        Args:
            prefix: Prefijo de las muestras (CH o CS)
            columns: Columnas de datos
            is_present: Indica si las muestras están presentes

        Returns:
            Dict[str, str]: Diccionario de muestras parseadas
        """
        if not is_present:
            return {}

        num_samples = 20 if prefix == "CH" else 21
        return {
            f"{prefix}{i+1}": columns[i] for i in range(min(num_samples, len(columns)))
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    async def _insert_entries_batch(
        self,
        entries: List[VCFEntryCreate],
        session: Optional[asyncio.ClientSession] = None,
    ) -> Dict[str, List]:
        """
        Inserta un lote de entradas en la base de datos.

        Args:
            entries: Lista de entradas a insertar
            session: Sesión de base de datos opcional

        Returns:
            Dict[str, List]: Diccionario con IDs insertados

        Raises:
            HTTPException: Si hay un error en la inserción
        """
        operations = [
            self.db.insert_one(entry.model_dump(), session=session) for entry in entries
        ]

        try:
            results = await asyncio.gather(*operations)
            inserted_ids = [str(result.inserted_id) for result in results]
            logger.info(f"Insertados exitosamente {len(inserted_ids)} documentos")
            return {"inserted_ids": inserted_ids}

        except Exception as e:
            logger.error(f"Error en inserción por lotes: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Error durante la inserción por lotes: {str(e)}",
            )

    async def process_vcf(self, file: UploadFile) -> Dict[str, List]:
        """
        Procesa un archivo VCF.

        Args:
            file: Archivo VCF a procesar

        Returns:
            Dict[str, List]: Resultado del procesamiento

        Raises:
            HTTPException: Si hay un error en el procesamiento
        """
        if not self._is_valid_vcf_file(file):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tipo de archivo inválido. Se esperaba un archivo VCF.",
            )

        start_time = datetime.utcnow()
        logger.info(f"Iniciando procesamiento de archivo: {file.filename}")

        try:
            async with await self.db.client.start_session() as session:
                async with session.start_transaction():
                    entries = []
                    total_processed = 0

                    while content := await file.read(self.chunk_size):
                        chunk_entries = list(
                            self.parse_vcf_chunk(content.decode("utf-8"))
                        )
                        entries.extend(chunk_entries)

                        if len(entries) >= self.batch_size:
                            await self._insert_entries_batch(
                                entries[: self.batch_size], session
                            )
                            total_processed += len(entries[: self.batch_size])
                            entries = entries[self.batch_size :]

                            logger.info(f"Procesadas {total_processed} entradas")

                    if entries:  # Procesar entradas restantes
                        await self._insert_entries_batch(entries, session)
                        total_processed += len(entries)

                    processing_time = (datetime.utcnow() - start_time).total_seconds()
                    logger.info(
                        f"Procesamiento completado. Total de entradas: {total_processed}. "
                        f"Tiempo: {processing_time:.2f} segundos"
                    )

                    return {
                        "status": "success",
                        "total_processed": total_processed,
                        "processing_time": processing_time,
                    }

        except Exception as e:
            logger.error(f"Error procesando archivo VCF: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error procesando archivo VCF: {str(e)}",
            )

    async def get_stats(self) -> Dict:
        """
        Obtiene estadísticas de las entradas VCF.

        Returns:
            Dict: Estadísticas de las entradas
        """
        try:
            total_entries = await self.db.count_documents({})

            pipeline = [
                {"$group": {"_id": "$chrom", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]

            chrom_stats = await self.db.aggregate(pipeline).to_list(None)

            return {
                "total_entries": total_entries,
                "entries_by_chromosome": {
                    stat["_id"]: stat["count"] for stat in chrom_stats
                },
            }

        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error obteniendo estadísticas: {str(e)}",
            )
