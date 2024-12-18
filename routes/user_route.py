import uuid
from fastapi import APIRouter, HTTPException, Depends
from schemas.user_schema import UserRegister
from services.publisher import publish_message
from services.auth import (
    find_user_by_email,
    create_user,
    verify_user,
)
from fastapi.security import OAuth2PasswordRequestForm

from utils.helper import create_access_token

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
        "message": "Usuario registrado exitosamente. Revisa tu correo para obtener la llave de seguridad."
    }


@router.post("/login")
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Autentica a un usuario y devuelve un token JWT.
    """
    user = find_user_by_email(form_data.username)  
    if not user or not verify_user(form_data.username, form_data.password):
        raise HTTPException(status_code=401, detail="Credenciales inválidas.")

    # Crear un token de acceso
    token = create_access_token({"sub": user["email"]})
    return {"access_token": token, "token_type": "bearer"}
