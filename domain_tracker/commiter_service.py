import asyncio
import json
import ipaddress
from itertools import chain
from collections import namedtuple

import asyncpg

import config
from domain_tracker.lib.rpc_consumer import RpcConsumer, AbstractHandler
from domain_tracker.lib.resolver import DnsQuery, DnsResponse


Host = namedtuple('Host', 'name type_id')
A = namedtuple('A', 'host ttl')
CNAME = namedtuple('CNAME', 'cname ttl')
MX = namedtuple('MX', 'host priority ttl')
NS = namedtuple('NS', 'host ttl')
SOA = namedtuple(
    'SOA',
    'nsname hostmaster serial refresh retry expires minttl ttl'
)


def get_host_type_id(host):
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return 2
    else:
        return 1


def get_response(obj):
    result = getattr(obj, 'response')
    if not result:
        return []
    else:
        return result


async def save_changes(dns_query: DnsQuery, pool):
    async with pool.acquire() as connection:
        async with connection.transaction():
            host_id = await connection.fetchval('SELECT id FROM host WHERE name=?', (dns_query.host, ))
            await connection.execute('''INSERT INTO scan(
            host_id, query_a_status, query_cname_status, query_mx_status,
            query_ns_status, query_soa_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                host_id,
                dns_query.a.status,
                dns_query.cname.status,
                dns_query.mx.status,
                dns_query.ns.status,
                dns_query.soa.status,
                dns_query.created_at
            )
            )
            hosts = [
                Host(query.host, get_host_type_id(query.host))
                for query in chain(
                    get_response(dns_query.a),
                    get_response(dns_query.aaaa),
                    get_response(dns_query.mx),
                    get_response(dns_query.ns)
                )
            ]
            if dns_query.cname.response:
                cname = dns_query.cname.response.cname
                hosts += [
                    Host(cname, get_host_type_id(cname))
                ]
            if dns_query.soa.response:
                soa_nsname = dns_query.soa.response.nsname
                hosts += [
                    Host(soa_nsname, get_host_type_id(soa_nsname))
                ]
            hosts = set(hosts)

            await connection.executemany(
                'INSERT OR IGNORE INTO host(name, type_id) VALUES (?, ?)',
                hosts
            )
            scan_id = await connection.fetchval(
                'SELECT id FROM scan WHERE host_id=(SELECT id FROM host WHERE name=?) ORDER BY julianday(created_at) DESC LIMIT 1',
                (dns_query.host, )
            )
            await connection.executemany(
                'INSERT INTO record_a(host_id, scan_id, ip_address_id, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?)',
                [(host_id, scan_id, a.host, a.ttl) for a in chain(get_response(dns_query.a), get_response(dns_query.aaaa))]
            )
            await connection.executemany(
                'INSERT INTO record_mx(host_id, scan_id, mail_id, priority, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?, ?)',
                [(host_id, scan_id, mx.host, mx.priority, mx.ttl) for mx in get_response(dns_query.mx)]
            )
            await connection.executemany(
                'INSERT INTO record_ns(host_id, scan_id, nameserver_id, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?)',
                [(host_id, scan_id, ns.host, ns.ttl) for ns in get_response(dns_query.ns)]
            )
            if dns_query.cname.response:
                await connection.execute(
                    'INSERT INTO record_cname(host_id, scan_id, cname, ttl) VALUES(?, ?, ?, ?)',
                    (
                        host_id,
                        scan_id,
                        dns_query.cname.response.cname,
                        dns_query.cname.response.ttl
                    )
                )
            if dns_query.soa.response:
                await connection.execute(
                    'INSERT INTO record_soa(host_id, scan_id, nameserver_id, hostmaster, serial, refresh, retry, expires, minttl, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?, ?, ?, ?, ?, ?, ?)',
                    (
                        host_id,
                        scan_id,
                        dns_query.soa.response.nsname,
                        dns_query.soa.response.hostmaster,
                        dns_query.soa.response.serial,
                        dns_query.soa.response.refresh,
                        dns_query.soa.response.retry,
                        dns_query.soa.response.expires,
                        dns_query.soa.response.minttl,
                        dns_query.soa.response.ttl
                    )
                )


def convert(data):
    for i, query in enumerate(data):
        for j, response in enumerate(query[2:], 2):
            if response[0] is not None:
                if j in (2, 3):
                    response[0] = [A(*record) for record in response[0]]
                elif j == 4:
                    response[0] = CNAME(*response[0])
                elif j == 5:
                    response[0] = [MX(*record) for record in response[0]]
                elif j == 6:
                    response[0] = [NS(*record) for record in response[0]]
                elif j == 7:
                    response[0] = SOA(*response[0])
            query[j] = DnsResponse(*response)
        data[i] = DnsQuery(*query)


class CommitterHandler(AbstractHandler):
    def __init__(self, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._pool = asyncpg.create_pool(**config.DATABASE)

    async def handle(self, channel, body, envelope, properties):
        message = json.loads(body.decode())
        convert(message)
        await asyncio.gather(
            [save_changes(response, self._pool) for response in message]
        )
        print('Committed')
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


def build_committer_service(loop):
    service = RpcConsumer(
        input_exchange_name='records',
        input_queue_name='records',
        handler=CommitterHandler(),
        loop=loop
    )
    loop.run_until_complete(service.connect())
    return service


def start_service():
    loop = asyncio.get_event_loop()
    service = build_committer_service(loop)
    loop.run_until_complete(service.start())
