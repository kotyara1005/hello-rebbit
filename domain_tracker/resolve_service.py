import json
import asyncio
from pprint import pprint

from domain_tracker.lib.rpc_consumer import RpcConsumer, AbstractHandler
from domain_tracker.lib.resolver import Resolver


class ResolveHandler(AbstractHandler):
    def __init__(self, *, output_exchange, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._resolver = Resolver(nameservers=['8.8.8.8'], loop=loop)
        self._output_exchange = output_exchange

    async def handle(self, channel, body, envelope, properties):
        message = json.loads(body)
        # message = body
        print(message)
        tasks = [self._resolver.query(domain) for domain in message]
        result = await asyncio.gather(*tasks, loop=self._loop)
        pprint(result)
        await channel.basic_publish(
            payload=json.dumps(result),
            exchange_name=self._output_exchange,
            routing_key=''
        )
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


def build_resolve_service(output_exchange, loop):
    service = RpcConsumer(
        input_exchange_name='domains',
        input_queue_name='domains',
        output_exchange_name=output_exchange,
        output_queue_name='records',
        handler=ResolveHandler(output_exchange=output_exchange, loop=loop),
        loop=loop
    )
    loop.run_until_complete(service.connect())
    return service


def start_service():
    loop = asyncio.get_event_loop()
    service = build_resolve_service('records', loop)
    loop.run_until_complete(service.start())
