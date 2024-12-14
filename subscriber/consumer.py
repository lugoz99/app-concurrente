import json
from operator import le
import os
import sys
import asyncio
from aio_pika import connect_robust

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.email import send_welcome_email  # Asegúrate de que esta función exista
from config.setting import settings


async def consume_messages():
    """Consume mensajes de la cola `user_registration`."""
    try:
        # Conexión robusta con RabbitMQ
        connection = await connect_robust("amqp://guest:guest@localhost/")
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("user_registration", durable=True)
            print("Cola `user_registration` declarada y lista para consumir mensajes.")
            print("Esperando mensajes...")
            async for message in queue:
                async with message.process():
                    try:
                        print(f"Mensaje recibido: {message.body.decode()}")
                        # Procesa el mensaje JSON
                        user_data = json.loads(message.decode())
                        email = user_data["email"]
                        security_key = user_data["security_key"]

                        # Enviar correo
                        print(f"Procesando usuario: {email}")
                        await send_welcome_email(email, security_key)

                    except json.JSONDecodeError:
                        print("Error al decodificar el mensaje JSON")
                    except KeyError as e:
                        print(f"Falta el campo esperado en el mensaje: {e}")
                    except Exception as e:
                        print(f"Error al procesar el mensaje: {e}")

    except Exception as e:
        print(f"Error en el consumidor de RabbitMQ: {str(e)}")


