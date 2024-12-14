from pydantic import BaseModel, EmailStr
from typing import Optional
class UserCreate(BaseModel):
    email: EmailStr
    role: Optional[str] = "user"  # Rol predeterminado

class UserResponse(BaseModel):
    email: EmailStr
    role: str
    message: str


