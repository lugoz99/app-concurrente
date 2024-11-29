from fastapi import FastAPI
from database.mongo import MongoDB
from routes.genoma_route import router as vcf_router


from contextlib import asynccontextmanager

db_instance = None  # Variable global para la instancia de MongoDB


# Definir un contexto asincrónico para el ciclo de vida de la app
@asynccontextmanager
async def lifespan(app: FastAPI):

    db_instance = MongoDB("mongodb://localhost:27017/", "genoma_db")
    # Evento de inicio
    print("Aplicación iniciada")
    yield
    # Evento de cierre
    print("Aplicación cerrada")
    db_instance.client.close()


# Crear la app usando el contexto de ciclo de vida
app = FastAPI(lifespan=lifespan)

# Registrar las rutas
app.include_router(vcf_router, prefix="/api", tags=["vcf"])
