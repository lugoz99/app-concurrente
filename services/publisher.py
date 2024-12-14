import aio_pika



async def publish_message(queue_name: str, message: str):
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        async with connection.channel() as channel:
            await channel.default_exchange.publish(
                aio_pika.Message(body=str(message).encode()), routing_key=queue_name
            )
            return True
    except Exception as e:
        print(f"Error al publicar mensaje: {e}")
        return False
