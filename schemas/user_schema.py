from pydantic import BaseModel, EmailStr


class UserRegister(BaseModel):
    email: EmailStr


class UserLogin(BaseModel):
    email: EmailStr
    security_key: str
    
class UserInDB(BaseModel):
    email: EmailStr
    security_key: str
    class Config:
        orm_mode = True
        