from passlib.context import CryptContext
import jwt
from datetime import datetime, timedelta

# Configuración de Passlib para cifrar contraseñas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
from config.setting import settings
SECRET_KEY = settings.SECRET_KEY # Cambia esto por algo más seguro
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES


def hash_password(password: str) -> str:
    """
    Cifra una contraseña.
    """
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifica si la contraseña ingresada coincide con la cifrada.
    """
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict) -> str:
    """
    Crea un token JWT con los datos proporcionados.
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_access_token(token: str) -> dict:
    """
    Verifica un token JWT y retorna los datos si es válido.
    """
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        return None
