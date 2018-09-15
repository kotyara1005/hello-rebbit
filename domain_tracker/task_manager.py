import asyncio
import json
from itertools import islice

import aioamqp
import asyncpg

import config
from domain_tracker.lib.utils import Closing
# TODO add namedtuple dump load
# TODO add logging

DB_NAME = 'dnsdb.sqlite3'


def iter_chunks(data,*, size):
    start = 0
    chunk = tuple(islice(data, start, start + size))
    while chunk:
        yield chunk
        start = start + size
        chunk = tuple(islice(data, start, start + size))


def _future_log_done_callback(ft):
    print('Future:', ft.result())


class RPCProducer:
    def __init__(self, *, exchange_name, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        self._loop = loop
        self._exchange_name = exchange_name

        self._loop.run_until_complete(self._connect())

    async def _connect(self):
        self._transport, self._protocol = await aioamqp.connect()
        self._channel = await self._protocol.channel()
        await self._channel.exchange_declare(
            exchange_name=self._exchange_name,
            type_name='fanout'
        )

    async def send(self, payload):
        print('Send')
        await self._channel.basic_publish(
            payload=payload,
            exchange_name=self._exchange_name,
            routing_key=''
        )

    async def stop(self):
        await self._protocol.close()
        self._transport.close()


class PeriodicTask:
    def __init__(self, interval, coroutine, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        self.handler = None
        self._loop = loop
        self.interval = interval
        self.coroutine = coroutine

    def stop(self):
        self.handler.cancel()


class TaskManagerService:
    def __init__(self, *, exchange_name, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        self._loop = loop
        self._tasks = []
        self._producer = RPCProducer(exchange_name=exchange_name)

    def start(self):
        for task in self._tasks:
            self._start_task(task)

    def add_task(self, interval: float, coroutine):
        self._tasks.append(
            PeriodicTask(interval, coroutine, loop=self._loop)
        )

    def _start_task(self, task):
        task.handler = asyncio.ensure_future(
            self._run_task(task),
            loop=self._loop
        )
        task.handler.add_done_callback(_future_log_done_callback)

    async def _run_task(self, task):
        while await asyncio.sleep(task.interval, True):
            print('In cycle!!!')
            result = await task.coroutine(loop=self._loop)
            for chunk in iter_chunks(result, size=5):
                await self._producer.send(json.dumps(chunk).encode())
        print('End')

    def stop(self):
        for task in self._tasks:
            task.stop()
        self._producer.stop()


async def get_domains(loop):
    async with Closing(asyncpg.connect(**config.DATABASE)) as connection:
        hosts = [
            row.name
            for row in await connection.execute(
                'SELECT name FROM host WHERE type_id=2'
            )
        ]
        return hosts


def build_service(loop) -> TaskManagerService:
    service = TaskManagerService(exchange_name='domains', loop=loop)
    service.add_task(interval=5, coroutine=get_domains)
    return service


def start_service():
    loop = asyncio.get_event_loop()
    service = build_service(loop)
    service.start()
    loop.run_forever()
