from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class ParallelConfig(BaseModel):
    """
    Configuración específica para el endpoint paralelo.
    """

    workers: Optional[int] = Field(
        None, description="Número de workers utilizados en el endpoint paralelo"
    )
    chunk_size: Optional[int] = Field(
        None, description="Tamaño de cada chunk en el endpoint paralelo"
    )


class ExecutionStats(BaseModel):
    """
    Representa las métricas de una ejecución individual de un endpoint.
    """

    endpoint: str = Field(
        ..., description="Nombre del endpoint (sequential o parallel)"
    )
    execution_time: float = Field(..., description="Tiempo de ejecución en segundos")
    timestamp: datetime = Field(..., description="Fecha y hora de la operación")
    page_size: int = Field(..., description="Tamaño de la página procesada")
    total_documents: int = Field(
        ..., description="Número total de documentos procesados"
    )
    notes: Optional[str] = Field(
        None, description="Notas adicionales sobre la ejecución"
    )
    parallel_config: Optional[ParallelConfig] = Field(
        None, description="Configuración específica para el endpoint paralelo"
    )

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "parallel",
                "execution_time": 4.12,
                "timestamp": "2024-12-14T10:00:00Z",
                "page_size": 1000,
                "total_documents": 10000,
                "notes": "Test with 4 workers and 500 chunk size",
                "parallel_config": {"workers": 4, "chunk_size": 500},
            }
        }


class ConfigurationStats(BaseModel):
    """
    Estadísticas agregadas para configuraciones específicas del endpoint paralelo.
    """

    workers: Optional[int] = Field(None, description="Número de workers utilizados")
    chunk_size: Optional[int] = Field(None, description="Tamaño de cada chunk")
    average_time: float = Field(
        ..., description="Tiempo promedio de ejecución en segundos"
    )
    min_time: float = Field(..., description="Tiempo mínimo registrado en segundos")
    max_time: float = Field(..., description="Tiempo máximo registrado en segundos")

    class Config:
        schema_extra = {
            "example": {
                "workers": 4,
                "chunk_size": 500,
                "average_time": 4.50,
                "min_time": 3.90,
                "max_time": 5.80,
            }
        }


class AggregatedStats(BaseModel):
    """
    Representa las estadísticas agregadas para un endpoint.
    """

    endpoint: str = Field(
        ..., description="Nombre del endpoint (sequential o parallel)"
    )
    average_time: float = Field(
        ..., description="Tiempo promedio de ejecución en segundos"
    )
    min_time: float = Field(..., description="Tiempo mínimo registrado en segundos")
    max_time: float = Field(..., description="Tiempo máximo registrado en segundos")
    runs: int = Field(..., description="Número total de ejecuciones registradas")
    configurations: Optional[List[ConfigurationStats]] = Field(
        None,
        description="Estadísticas de configuraciones específicas para el endpoint paralelo",
    )

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "parallel",
                "average_time": 5.34,
                "min_time": 3.90,
                "max_time": 7.45,
                "runs": 20,
                "configurations": [
                    {
                        "workers": 4,
                        "chunk_size": 500,
                        "average_time": 4.50,
                        "min_time": 3.90,
                        "max_time": 5.80,
                    },
                    {
                        "workers": 8,
                        "chunk_size": 1000,
                        "average_time": 6.20,
                        "min_time": 5.50,
                        "max_time": 7.45,
                    },
                ],
            }
        }
