import os
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()


# Definir las variables de configuración
class Settings:
    MONGO_URI: str = os.getenv("MONGO_URI")
    MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME")
    NUM_PROCESSES: int = int(os.getenv("NUM_PROCESSES", 8))
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", 10000))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 1800))
    SECRET_KEY:str = os.getenv('SECRET_KEY')
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))
    RABBITMQ_HOST:str = os.getenv("RABBITMQ_HOST")
    RABBITMQ_QUEUE: str = os.getenv("RABBITMQ_QUEUE")
    MAIL_USERNAME: str = os.getenv("MAIL_USERNAME")
    MAIL_PASSWORD: str = os.getenv("MAIL_PASSWORD")
    MAIL_FROM: str = os.getenv("MAIL_FROM")
    MAIL_PORT: int = int(os.getenv("MAIL_PORT"))
    MAIL_SERVER: str = os.getenv("MAIL_SERVER")
    ALGORITHM : str = os.getenv("ALGORITHM")


# Crear una instancia de la clase Settings
settings = Settings()
