from pymongo import MongoClient
from pymongo.collection import Collection
from config.setting import settings


class Database:
    _client: MongoClient | None = None
    _db = None
    @staticmethod
    def connect() -> None:
        """
        Connects to the MongoDB server using pymongo with the specified configuration.
        """
        try:
            Database._client = MongoClient(
                settings.MONGO_URI,  # URI de conexión desde .env
                maxPoolSize=None,
                connectTimeoutMS=30000,
                socketTimeoutMS=None,
                connect=False,
                w=1,  # Write concern reducido
                journal=False,
                compressors="snappy",
            )
            Database._db = Database._client[settings.MONGO_DB_NAME]
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

    @staticmethod
    def close() -> None:
        """
        Closes the connection to the MongoDB server.
        """
        if Database._client is not None:
            Database._client.close()
            Database._client = None
            Database._db = None
        else:
            raise ConnectionError("Client not connected")

    @staticmethod
    def get_db():
        """
        Returns the connected database instance.
        """
        if Database._db is not None:
            return Database._db
        else:
            raise ConnectionError("Database not connected")

    @staticmethod
    def get_collection(collection_name: str) -> Collection:
        """
        Returns the collection from the connected database.
        If the collection does not exist, it will be created automatically when you insert data.
        If you want to ensure that it is created explicitly, this method will create it for you.
        """
        db = Database.get_db()  # Obtener la base de datos
        collection: Collection = db[collection_name]  # Obtener la colección por nombre

        # Verificar si la colección existe; si no, crearla
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)

        return collection

    @staticmethod
    def create_collection(collection_name: str) -> None:
        """
        Creates a collection explicitly with the given name.
        """
        db = Database.get_db()
        db.create_collection(collection_name)

