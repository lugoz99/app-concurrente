from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from config.setting import settings


class Database:
    _client: AsyncIOMotorClient | None = None
    _db: AsyncIOMotorDatabase | None = None

    @staticmethod
    def connect() -> None:

        Database._client = AsyncIOMotorClient(settings.MONGO_URI)
        Database._db = Database._client[settings.MONGO_DB_NAME]

    @staticmethod
    def close() -> None:
        if Database._client is not None:
            Database._client.close()
        else:
            raise ConnectionError("Client not connected")

    @staticmethod
    def get_db() -> AsyncIOMotorDatabase:
        if Database._db is not None:
            return Database._db
        else:
            raise ConnectionError("Database not connected")
