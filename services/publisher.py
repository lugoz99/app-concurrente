import aio_pika
from config.setting import settings



async def publish_message(queue_name: str, message: str):
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        async with connection.channel() as channel:
            await channel.default_exchange.publish(
                aio_pika.Message(body=str(message).encode()), routing_key=queue_name
            )
    except Exception as e:
        raise RuntimeError(f"Error al publicar mensaje: {e}")
