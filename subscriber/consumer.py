import os
import sys
import asyncio
from aio_pika import connect_robust
#* TODO : HACER QUE CORRA EN SEGUNDO PLANO
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.email import send_welcome_email  # Asegúrate de que esta función exista


async def consume_messages():
    print("Intentando conectar a RabbitMQ...")
    # Conexión directa con RabbitMQ
    connection = await connect_robust("amqp://guest:guest@localhost/")
    print("Conexión exitosa a RabbitMQ")
    async with connection:
        channel = await connection.channel()
        print("Canal creado, declarando cola...")
        queue = await channel.declare_queue("user.register", durable=True)
        print("Cola declarada y lista para consumir mensajes")

        async for message in queue:
            async with message.process():
                try:
                    print(f"Mensaje recibido: {message.body.decode()}")
                    data = eval(message.body.decode())  # Deserializar mensaje
                    # Envía el correo utilizando send_welcome_email
                    print(
                        f"Enviando correo a {data['email']} con llave {data['key']}..."
                    )
                    await send_welcome_email(data["email"], data["key"])
                    print(f"Correo enviado a {data['email']}")
                except Exception as e:
                    print(f"Error al procesar mensaje: {e}")


# Ejecutar suscriptor
if __name__ == "__main__":
    print("Iniciando suscriptor...")
    asyncio.run(consume_messages())
