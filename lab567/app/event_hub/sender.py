from app import app
from app.constants import Config
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


async def send_data(data):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=Config.EVENT_HUB_CONNNECTION_STRING, eventhub_name=Config.EVENT_HUB_NAME)

    async with producer:

        event_data_batch = await producer.create_batch()
        event_data_batch.add(EventData(data))
        print('data')
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()