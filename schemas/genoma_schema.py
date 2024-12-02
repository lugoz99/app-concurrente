from pydantic import BaseModel, Field
from typing import Dict, Optional
from bson import ObjectId


# Helper para manejar el ObjectId de MongoDB con pydantic


# Modelo base para las entradas VCF
class VCFEntryBase(BaseModel):
    chrom: str
    pos: int
    ref: str
    alt: str
    qual: str
    filter: str
    info: str
    format: str

    # Muestras organizadas en diccionarios dinámicos
    samples_CH: Optional[Dict[str, Optional[str]]] = Field(
        default_factory=dict
    )  # Ejemplo: {"CH1": "0|1", "CH2": "1|0"}
    samples_CS: Optional[Dict[str, Optional[str]]] = Field(
        default_factory=dict
    )  # Ejemplo: {"CS1": "0|0", "CS2": "1|1"}


# Modelo para crear una entrada VCF
class VCFEntryCreate(VCFEntryBase):
    pass
