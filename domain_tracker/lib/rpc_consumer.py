"""RPC client, aioamqp implementation of RPC"""

import abc
import asyncio

import aioamqp


async def declare_and_bind(channel, exchange_name, queue_name):
        await channel.exchange_declare(
            exchange_name=exchange_name,
            type_name='fanout'
        )
        await channel.queue_declare(queue_name=queue_name)
        await channel.queue_bind(
            exchange_name=exchange_name,
            queue_name=queue_name,
            routing_key=''
        )


class AbstractHandler(abc.ABC):
    @abc.abstractmethod
    async def handle(self, channel, body, envelope, properties):
        raise NotImplemented()


def bicondition(a: bool, b: bool) -> bool:
    """
    >>> bicondition(True, True)
    True
    >>> bicondition(True, False)
    False
    >>> bicondition(False, True)
    False
    >>> bicondition(False, False)
    True
    """
    return (a and b) or (not a and not b)


class RpcConsumer:
    def __init__(
            self,
            *,
            input_exchange_name: str,
            input_queue_name: str,
            output_exchange_name: str=None,
            output_queue_name: str=None,
            handler: AbstractHandler,
            loop=None,
            chanel_check_delay=10):
        is_valid_output_params = not(
            bicondition(
                bool(output_exchange_name),
                bool(output_queue_name)
            )
        )
        if is_valid_output_params:
            raise RuntimeError('Bad output parameters')
        if loop is None:
            loop = asyncio.get_event_loop()

        self._loop = loop
        self._input_exchange_name = input_exchange_name
        self._input_queue_name = input_queue_name
        self._output_exchange_name = output_exchange_name
        self._output_queue_name = output_queue_name
        self._transport = None
        self._protocol = None
        self._channel = None
        self._handler = handler
        self.chanel_check_delay = chanel_check_delay

    async def connect(self):
        """ an `__init__` method can't be a coroutine"""
        self._transport, self._protocol = await aioamqp.connect()
        self._channel = await self._protocol.channel()

        await declare_and_bind(
            self._channel,
            self._input_exchange_name,
            self._input_queue_name
        )
        if self._output_exchange_name and self._output_queue_name:
            await declare_and_bind(
                self._channel,
                self._output_exchange_name,
                self._output_queue_name
            )

    async def start(self):
        await self._channel.basic_consume(
            self._handler.handle,
            queue_name=self._input_queue_name,
        )

        while self._channel.is_open:
            await asyncio.sleep(self.chanel_check_delay)
