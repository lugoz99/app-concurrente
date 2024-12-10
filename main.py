from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from routes.file_route import router

# Configuración de CORS
origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:4200",
    # Agrega aquí otros orígenes permitidos
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Permitir orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permitir todos los encabezados
)

# Registrar las rutas
app.include_router(router, prefix="/genoma", tags=["file_upload"])


@app.get("/")
async def root():
    """Endpoint de prueba."""
    return {"message": "FastAPI server running correctly."}


if __name__ == "__main__":
    print(
        "El servidor FastAPI está en ejecución. Puedes presionar Ctrl+C para detenerlo."
    )

    # Configura y corre el servidor con uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
