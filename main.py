from contextlib import asynccontextmanager
from fastapi import FastAPI
from threading import Thread
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from database.mongo import Database
from routes.file_route import router
from routes.user_route import router as user_router
from subscriber.consumer import start_consumer_in_thread


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Maneja el ciclo de vida de la aplicación.
    """
    db_connected = False
    consumer_thread = None
    try:
        # Conexión a la base de datos
        Database.connect()
        db_connected = True
        print("Conexión a MongoDB establecida.")

        # Inicia el consumidor en un hilo
        consumer_thread = Thread(target=start_consumer_in_thread, daemon=True)
        consumer_thread.start()
        print("Consumidor de RabbitMQ inicializado en un hilo separado.")

        # Permite que la aplicación acepte solicitudes
        yield
    except Exception as e:
        print(f"Error durante la inicialización: {e}")
        raise e
    finally:
        # Finalización de recursos
        if db_connected:
            Database.close()
            print("Conexión a MongoDB cerrada.")
        if consumer_thread and consumer_thread.is_alive():
            print("Deteniendo consumidor...")
            consumer_thread.join(timeout=1)


# Crear instancia de FastAPI
app = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)

# Rutas y middleware
app.include_router(router, prefix="/genoma", tags=["file_upload"])
app.include_router(user_router, prefix="/auth", tags=["Auth"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "FastAPI server running correctly."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
