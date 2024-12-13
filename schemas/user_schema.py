from pydantic import BaseModel, EmailStr, Secret


class UserRegister(BaseModel):
    email: EmailStr


class UserLogin(BaseModel):
    email: EmailStr
    security_key: Secret
    
class UserInDB(BaseModel):
    email: EmailStr
    security_key: Secret
    class Config:
        orm_mode = True
        