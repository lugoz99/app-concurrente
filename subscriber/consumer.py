from aio_pika import connect_robust
from utils.email import send_welcome_email  # Asegúrate de que esta función exista

# Broker
async def consume_messages():
    """
    Consumidor de mensajes de RabbitMQ.
    Escucha la cola 'user.register' y envía correos electrónicos a los usuarios registrados.
    """
    print("Intentando conectar a RabbitMQ...")
    try:
        # Conexión directa con RabbitMQ
        connection = await connect_robust("amqp://guest:guest@localhost/")
        print("Conexión exitosa a RabbitMQ")

        async with connection:
            channel = await connection.channel()
            print("Canal creado, declarando cola...")
            queue = await channel.declare_queue("user.register", durable=True)
            print("Cola declarada y lista para consumir mensajes")

            # Consumir mensajes
            async for message in queue:
                async with message.process():
                    try:
                        print(f"Mensaje recibido: {message.body.decode()}")
                        data = eval(message.body.decode())  # Deserializar mensaje
                        # Enviar correo utilizando send_welcome_email
                        print(
                            f"Enviando correo a {data['email']} con llave {data['key']}..."
                        )
                        await send_welcome_email(data["email"], data["key"])
                        print(f"Correo enviado a {data['email']}")
                    except Exception as e:
                        print(f"Error al procesar mensaje: {e}")

    except Exception as e:
        print(f"Error al conectar a RabbitMQ: {e}")
