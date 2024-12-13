import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from database.mongo import Database
from routes.file_route import router
from routes.user_route import router as user_router
from subscriber.consumer import consume_messages

# Configurar los orígenes permitidos
origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:4200",
    # Agrega aquí otros orígenes permitidos
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Contexto de ciclo de vida de la aplicación.
    Maneja la conexión y desconexión de MongoDB.
    """
    try:
        # Conexión a la base de datos al inicio
        Database.connect()
        print("Conexión a MongoDB establecida.")
        asyncio.create_task(consume_messages())  # Inicia la tarea de consumir mensajes
        yield  # Permitir que la aplicación corra
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
    finally:
        try:
            # Cerrar la conexión a la base de datos al finalizar
            Database.close()
            print("Conexión a MongoDB cerrada.")
        except Exception as e:
            print(f"Error al cerrar la conexión a MongoDB: {e}")


# Instancia principal de FastAPI
app = FastAPI(
    default_response_class=ORJSONResponse, lifespan=lifespan, strict_slashes=False
)

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
