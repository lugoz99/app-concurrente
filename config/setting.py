from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str
    MONGO_DB_NAME: str
    CHUNK_SIZE: int = 12000  # Tamaño de chunk para procesamiento
    BATCH_SIZE: int = 1800  # Tamaño de batch para inserción
    ENABLE_PARALLEL_SEARCH: bool = True
    ENABLE_PARALLEL_SORT: bool = True

    class Config:
        env_file = ".env"


# Crear una instancia de Settings
settings = Settings()
