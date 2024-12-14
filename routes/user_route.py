import json
import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from schemas.user_schema import UserCreate, UserResponse
from services.publisher import publish_message
from  services.auth import create_access_token, find_user_by_email, create_user, hash_security_key, verify_security_key

router = APIRouter()


@router.post("/register", response_model=UserResponse)
async def register_user(user: UserCreate):
    """
    Registra un nuevo usuario en la base de datos y publica un mensaje en RabbitMQ.
    """
    # Verificar si el usuario ya existe
    if find_user_by_email(user.email):
        raise HTTPException(status_code=400, detail="El usuario ya existe.")

    security_key = str(uuid.uuid4())
    # Hashea la `security_key` y guarda al usuario en la base de datos
    hashed_key = hash_security_key(security_key)
    new_user = {"email": user.email, "role": user.role, "security_key": hashed_key}
    if not create_user(new_user):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo registrar al usuario",
        )
    try:
        await publish_message(
            queue_name="user_registration",
            message=json.dumps(
                {"email": user.email, "security_key": security_key}
            )
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar mensaje: {e}")

    return UserResponse(
        email=user.email,
        role=user.role,
        message="Usuario registrado exitosamente. Se ha enviado un correo con la llave de seguridad.",
    )


@router.post("/login")
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Ruta de inicio de sesión.
    Verifica las credenciales del usuario y genera un token JWT.
    """
    try:
        # Busca al usuario por correo (username en el formulario)
        user = find_user_by_email(form_data.username)
        if not user or not verify_security_key(
            form_data.password, user["security_key"]
        ):
            raise HTTPException(status_code=401, detail="Credenciales inválidas.")

        # Genera un token de acceso JWT
        access_token = create_access_token(
            data={"sub": user["email"], "role": user["role"]}
        )
        return {"access_token": access_token, "token_type": "bearer"}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al iniciar sesión: {str(e)}"
        )
