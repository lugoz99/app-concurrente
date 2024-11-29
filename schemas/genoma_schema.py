from pydantic import BaseModel, Field
from typing import Dict, Optional
from bson import ObjectId



# Helper para manejar el ObjectId de MongoDB con pydantic
class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(type="string")


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


# Modelo de salida para las entradas VCF (con id de MongoDB)
class VCFEntryOut(VCFEntryBase):
    id: PyObjectId = Field(alias="_id")

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}
        orm_mode = True
