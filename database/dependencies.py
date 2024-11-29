from .mongo import MongoDB


from config.setting import settings


def get_database() -> MongoDB:
    global db_instance
    if db_instance is None:
        db_instance = MongoDB(settings.MONGO_URI, settings.MONGO_DB_NAME)
    return db_instance
