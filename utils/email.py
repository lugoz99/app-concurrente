from pathlib import Path
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from config.setting import settings
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de conexión para FastAPI-Mail
conf = ConnectionConfig(
    MAIL_USERNAME=settings.MAIL_USERNAME,
    MAIL_PASSWORD=settings.MAIL_PASSWORD,
    MAIL_FROM=settings.MAIL_FROM,
    MAIL_PORT=settings.MAIL_PORT,
    MAIL_SERVER=settings.MAIL_SERVER,
    MAIL_STARTTLS=True,  
    MAIL_SSL_TLS=False,  
    USE_CREDENTIALS=True, 
    VALIDATE_CERTS=True, 
)

template_path = str(Path(__file__).parent.parent / "templates")

try:
    template_env = Environment(
        loader=FileSystemLoader(template_path)  # Ajustar la ruta
    )

    # Verificar si la plantilla existe
    template = template_env.get_template("welcome.html")
    print(f"Plantilla 'welcome.html' cargada correctamente desde: {template_path}")

except TemplateNotFound as e:
    print(f"Error: No se encontró la plantilla 'welcome.html' en {template_path}.")
    raise e  # Lanza el error si no se encuentra la plantilla
except Exception as e:
    print(f"Error al cargar las plantillas desde {template_path}: {e}")
    raise e  # Lanza cualquier otro error

# Función para enviar correos electrónicos
async def send_welcome_email(email: str, security_key: str):
    # Cargar y renderizar la plantilla HTML con Jinja2
    template = template_env.get_template("welcome.html")
    html_content = template.render(username=email, security_key=security_key)

    # Crear el mensaje con contenido HTML
    message = MessageSchema(
        subject="Your Security Key",
        recipients=[email],  # Lista de destinatarios
        body=html_content,  # Cuerpo del correo en formato HTML
        subtype="html",  # Indicar que es un correo HTML
    )

    # Enviar el correo
    fast_mail = FastMail(conf)
    await fast_mail.send_message(message)
