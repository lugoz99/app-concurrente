from pydantic import BaseModel




class UserRegister(BaseModel):
    email: str


class UserLogin(BaseModel):
    email: str
    security_key: str
