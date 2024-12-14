from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from database.mongo import Database
from fastapi import Depends, HTTPException, status
from pymongo.errors import PyMongoError


from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from config.setting import settings

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Generar un hash de la security_key
def hash_security_key(security_key: str) -> str:
    return pwd_context.hash(security_key)


# Verificar que una security_key coincida con el hash almacenado
def verify_security_key(security_key: str, hashed_key: str) -> bool:
    return pwd_context.verify(security_key, hashed_key)


# Crear un token JWT
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def get_users_collection():
    """
    Obtiene la colección 'users' de la base de datos.
    """
    return Database.get_collection("users")


# Dependencia para obtener al usuario actual desde el token JWT
def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        return payload  # Retorna los datos del usuario
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )


# Dependencia para validar que el usuario tenga un rol específico
def has_role(required_role: str):
    def role_checker(user: dict = Depends(get_current_user)):
        if user.get("role") != required_role:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"No tienes permisos para acceder a esta ruta (se requiere rol: {required_role}).",
            )
        return user

    return role_checker


def create_user(user: dict) -> bool:
    """Guarda un nuevo usuario en la base de datos."""
    users_collection = get_users_collection()
    try:
        result = users_collection.insert_one(user)
        return result.acknowledged
    except Exception:
        return False


def find_user_by_email(email: str):
    """Busca un usuario por su correo."""
    try:
        return get_users_collection().find_one({"email": email})
    except Exception:
        return None
