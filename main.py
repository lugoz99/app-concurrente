from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
import asyncio
from database.mongo import Database
from routes.file_route import router
from routes.user_route import router as user_router
from subscriber.consumer import consume_messages

# Configurar los orígenes permitidos
origins = ["*"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Contexto de ciclo de vida de la aplicación.
    Maneja la conexión y desconexión de MongoDB y la tarea del consumidor RabbitMQ.
    """
    try:
        # Conexión a MongoDB
        Database.connect()
        print("Conexión a MongoDB establecida.")

        # Inicia la tarea de consumir mensajes
        task = asyncio.create_task(consume_messages())
        print("Tarea del consumidor RabbitMQ iniciada.")

        yield  # Permitir que la aplicación corra
    finally:
        # Cancelar la tarea del consumidor RabbitMQ
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            print("Tarea del consumidor RabbitMQ cancelada.")

        # Cerrar la conexión a MongoDB
        Database.close()
        print("Conexión a MongoDB cerrada.")


# Instancia principal de FastAPI
app = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)

# Incluir rutas
app.include_router(router, prefix="/genoma", tags=["file_upload"])
app.include_router(user_router, prefix="/auth", tags=["Auth"])

# Agregar middleware de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Permitir orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permitir todos los encabezados
)


@app.get("/")
async def root():
    """Endpoint de prueba."""
    return {"message": "FastAPI server running correctly."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
