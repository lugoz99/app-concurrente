import uuid
from fastapi import APIRouter, HTTPException
from schemas.user_schema import UserRegister
from services.publisher import publish_message
from  services.auth import find_user_by_email, create_user
from database.mongo import Database

router = APIRouter()


@router.post("/register")
async def register_user(user: UserRegister):
    """
    Registra un nuevo usuario en la base de datos y publica un mensaje en RabbitMQ.
    """

    # Verificar si el usuario ya existe
    if find_user_by_email(user.email):
        raise HTTPException(status_code=400, detail="El usuario ya existe.")
    
    security_key = str(uuid.uuid4())

    try:
        if not create_user(user.email, security_key):
            raise HTTPException(status_code=500, detail="Error al registrar usuario.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al registrar usuario: {e}")
    try:
        # Publicar el evento en RabbitMQ
        await publish_message({"email": user.email, "key": security_key})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar mensaje: {e}")

    return {
        "message": "User registered successfully. Check your email for the security key."
    }


# @router.post("/login")
# async def login(data: LoginRequest):
#     if verify_user(data.email, data.security_key):
#         token = create_access_token({"sub": data.email})
#         return {"access_token": token, "token_type": "bearer"}
#     raise HTTPException(status_code=401, detail="Credenciales incorrectas.")


# @router.get("/protected")
# async def protected_route(user: str = Depends(get_current_user)):
#     return {"message": f"Hola, {user}. Accediste a una ruta protegida."}
