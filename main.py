from contextlib import asynccontextmanager
from re import A
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import signal
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from database.mongo import Database
from routes.file_route import router
from config.setting import settings

# Configuración de CORS
origins = [
    "http://localhost",
    "http://localhost:8000",
    # Agrega aquí otros orígenes permitidos
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    Database.connect()
    yield
    Database.close()


app = FastAPI(lifespan=lifespan)


app = FastAPI(lifespan=lifespan)
app.router.lifespan_context = lifespan
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Permitir orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permitir todos los encabezados
)


# Registrar las rutas
app.include_router(router, prefix="/genoma", tags=["file_upload"])


# Manejo de la interrupción (Ctrl+C)
def handle_interrupt(signal, frame):
    """Maneja la interrupción del proceso (Ctrl+C)."""
    print("\nProceso cancelado por el usuario.")
    sys.exit(0)


# Vincula la señal de interrupción (Ctrl+C) con el manejador
signal.signal(signal.SIGINT, handle_interrupt)


@app.get("/")
async def root():
    print(
        f"Connected to MongoDB: with num_processes={settings.NUM_PROCESSES}, chunk_size={settings.CHUNK_SIZE}"
    )
    return {"message": "FastAPI server running correctly."}


if __name__ == "__main__":
    print(
        "El servidor FastAPI está en ejecución. Puedes presionar Ctrl+C para detenerlo."
    )

    # Configura y corre el servidor con uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
