from pydantic import BaseSettings


class Settings(BaseSettings):
    MONGO_URI: str = "mongodb://localhost:27017/"
    MONGO_DB_NAME: str = "genoma_db"


settings = Settings()
