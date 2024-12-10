from pymongo import MongoClient
from pymongo.database import Database as PyMongoDatabase
from config.setting import settings


class Database:
    _client: MongoClient | None = None
    _db: PyMongoDatabase | None = None

    @staticmethod
    def connect() -> None:
        """
        Connects to the MongoDB server using pymongo with the specified configuration.
        """
        Database._client = MongoClient(
            settings.MONGO_URI,
            maxPoolSize=None,
            connectTimeoutMS=30000,
            socketTimeoutMS=None,
            connect=False,
            w=1,  # Write concern reducido
            journal=False,
            compressors="snappy",
        )
        Database._db = Database._client[settings.MONGO_DB_NAME]

    @staticmethod
    def close() -> None:
        """
        Closes the connection to the MongoDB server.
        """
        if Database._client is not None:
            Database._client.close()
        else:
            raise ConnectionError("Client not connected")

    @staticmethod
    def get_db() -> PyMongoDatabase:
        """
        Returns the connected database instance.
        """
        if Database._db is not None:
            return Database._db
        else:
            raise ConnectionError("Database not connected")
