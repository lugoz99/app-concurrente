import uuid
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from database.mongo import Database
from utils.helper import (
    hash_password,
    verify_password,
    verify_access_token,
)
from pymongo.errors import PyMongoError

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def get_users_collection():
    """
    Obtiene la colección 'users' de la base de datos.
    """
    return Database.get_collection("users")


def create_user(email: str, security_key: str) -> bool:
    """
    Crea un usuario en la base de datos.
    Devuelve True si la inserción fue exitosa, False en caso contrario.
    """
    users_collection = get_users_collection()
    user = {
        "email": email,
        "security_key": hash_password(security_key),  # Almacena la llave cifrada
    }
    try:
        result = users_collection.insert_one(user)
        return result.acknowledged  # Retorna True si la inserción fue exitosa
    except Exception:
        return False  # En caso de error, devuelve False


def find_user_by_email(email: str):
    """
    Busca un usuario por correo electrónico y devuelve el documento si existe.
    """
    try:
        return get_users_collection().find_one({"email": email})
    except PyMongoError:
        return None


def verify_user(email: str, security_key: str) -> bool:
    """
    Verifica las credenciales de un usuario.
    """
    user = find_user_by_email(email)
    if not user:
        return False
    return verify_password(security_key, user["security_key"])


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    """
    Verifica el token JWT y retorna el email del usuario si es válido.
    """
    payload = verify_access_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")
    return payload["sub"]
