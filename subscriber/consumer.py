import asyncio
import json
from aio_pika import connect_robust, IncomingMessage
from config.setting import settings
from utils.email import send_welcome_email  # Asegúrate de que esta función exista


async def process_message(message: IncomingMessage):
    """Procesa un mensaje recibido de la cola."""
    try:
        print(f"Mensaje recibido: {message.body.decode()}")
        user_data = json.loads(message.body.decode())
        email = user_data["email"]
        security_key = user_data["security_key"]

        # Enviar correo
        print(f"Procesando usuario: {email}")
        await send_welcome_email(email, security_key)
        print(f"Correo enviado exitosamente a: {email}")

    except json.JSONDecodeError:
        print("Error: Mensaje no tiene formato JSON válido.")
    except KeyError as e:
        print(f"Error: Falta el campo esperado en el mensaje: {e}")
    except Exception as e:
        print(f"Error inesperado al procesar el mensaje: {e}")


async def consume_messages():
    """Consume mensajes de la cola `user_registration`."""
    try:
        print("Inicializando conexión a RabbitMQ...")
        connection = await connect_robust(settings.RABBITMQ_URI)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("user_registration", durable=True)
            print("Consumidor conectado y listo para procesar mensajes.")

            # Consumir mensajes de forma asíncrona
            async for message in queue:
                async with message.process():
                    await process_message(message)
    except Exception as e:
        print(f"Error en el consumidor de RabbitMQ: {e}")


def start_consumer_in_thread():
    """Inicia el consumidor en un bucle de eventos dentro de un hilo."""
    loop = asyncio.new_event_loop()  # Crea un nuevo bucle de eventos
    asyncio.set_event_loop(loop)  # Asigna este bucle al hilo actual
    loop.run_until_complete(consume_messages())  # Ejecuta el consumidor en este bucle
