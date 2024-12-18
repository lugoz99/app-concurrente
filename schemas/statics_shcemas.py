from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class PerformanceMetrics(BaseModel):
    field: str  # El campo por el que se filtra (ej. "CHROM")
    value: str  # El valor por el que se filtra (ej. "1")
    type: str  # Tipo de consulta: "paralela" o "secuencial"
    num_workers: Optional[int] = None  # Solo para consultas paralelas
    chunk_size: Optional[int] = None  # Solo para consultas paralelas
    query_time: float  # Tiempo total de la consulta en segundos
    documents_retrieved: int  # Número de documentos recuperados
    execution_method: str  # Método de ejecución: "paralela" o "secuencial"
    start_time: datetime  # Hora de inicio de la consulta
    end_time: datetime  # Hora de finalización de la consulta

    class Config:
        orm_mode = True  # Permite que se use con bases de datos ORM
