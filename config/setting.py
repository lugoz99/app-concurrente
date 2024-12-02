from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str
    MONGO_DB_NAME: str
    NUM_PROCESSES: int = 8  # Número de procesos paralelos
    CHUNK_SIZE: int = 10000  # Tamaño de chunk para procesamiento
    MAX_WORKERS: int = 12  # Máximo de workers
    BATCH_SIZE: int = 1000  # Tamaño de batch para inserción
    ENABLE_PARALLEL_SEARCH: bool = True
    ENABLE_PARALLEL_SORT: bool = True

    class Config:
        env_file = ".env"


# Crear una instancia de Settings
settings = Settings()
