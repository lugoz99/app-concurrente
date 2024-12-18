import aio_pika

from dotenv import load_dotenv
load_dotenv()

async def publish_message(message: dict):
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=str(message).encode()), routing_key="user.register"
        )
        print("Mensaje publicado en RabbitMQ")
