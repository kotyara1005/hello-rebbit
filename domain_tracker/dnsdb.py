# coding: utf-8
import os
import sqlite3
import asyncio
import contextlib
import ipaddress
from collections import namedtuple
from itertools import chain
from pprint import pprint
from datetime import datetime

import aiodns
import requests

DB_NAME = 'dnsdb.sqlite3'


DnsQuery = namedtuple('DnsRequest',
                      'host created_at a, aaaa, cname, mx, ns, soa')
DnsResponse = namedtuple('DnsResponse', 'response status')
Host = namedtuple('Host', 'name type_id')


class Resolver:
    def __init__(self, *args, **kwargs):
        self._resolver = aiodns.DNSResolver(*args, **kwargs)

    async def single_query(self, host, qtype):
        try:
            return DnsResponse(
                await self._resolver.query(host, qtype),
                'OK'
            )
        except (UnicodeError, aiodns.error.DNSError):
            # TODO handle errors
            return DnsResponse(None, 'FAIL')
        except:
            return DnsResponse(None, 'FAIL')

    async def query(self, domain):
        print(domain)
        now = datetime.utcnow().isoformat(' ')
        return DnsQuery(
            domain,
            now,
            await self.single_query(domain, 'A'),
            await self.single_query(domain, 'AAAA'),
            await self.single_query(domain, 'CNAME'),
            await self.single_query(domain, 'MX'),
            await self.single_query(domain, 'NS'),
            await self.single_query(domain, 'SOA')
        )


def get_new_domains():
    # yield from ['google.com']
    # return
    response = requests.get(
        'https://isc.sans.edu/feeds/suspiciousdomains_High.txt'
    )
    response.raise_for_status()

    yield from (
        line
        for line in response.text.split('\n')
        if not line.startswith('#') and '.' in line
    )


def get_db():
    """Opens a new database connection"""
    rv = sqlite3.connect(DB_NAME)
    rv.row_factory = sqlite3.Row
    return rv


def create_db():
    """Initializes the database.
    >>> create_db()
    """
    with contextlib.closing(get_db()) as db:
        cursor = db.cursor()
        with open('schema.sql', mode='r') as file:
            cursor.executescript(file.read())
        db.commit()


def remove_db():
    os.remove(DB_NAME)


def add_domains():
    with contextlib.closing(get_db()) as db:
        cursor = db.cursor()
        cursor.executemany(
            'INSERT INTO host(name, type_id) VALUES (?, ?)',
            [(domain, 2) for domain in get_new_domains()]
        )
        db.commit()


def check_domains(domains):
    resolver = Resolver()
    loop = asyncio.get_event_loop()
    tasks = [asyncio.ensure_future(resolver.query(domain)) for domain in domains]
    # TODO meke generator
    return loop.run_until_complete(asyncio.gather(*tasks))


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


def save_changes(dns_query: DnsQuery):
    with contextlib.closing(get_db()) as db:
        cursor = db.cursor()
        r = cursor.execute('SELECT id FROM host WHERE name=?', (dns_query.host, ))
        host_id = list(r)[0][0]
        cursor.execute('''INSERT INTO scan(
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

        cursor.executemany(
            'INSERT OR IGNORE INTO host(name, type_id) VALUES (?, ?)',
            hosts
        )
        r = cursor.execute(
            'SELECT id FROM scan WHERE host_id=(SELECT id FROM host WHERE name=?) ORDER BY julianday(created_at) DESC LIMIT 1',
            (dns_query.host, )
        )
        scan_id = list(r)[0][0]
        cursor.executemany(
            'INSERT INTO record_a(host_id, scan_id, ip_address_id, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?)',
            [(host_id, scan_id, a.host, a.ttl) for a in chain(get_response(dns_query.a), get_response(dns_query.aaaa))]
        )
        cursor.executemany(
            'INSERT INTO record_mx(host_id, scan_id, mail_id, priority, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?, ?)',
            [(host_id, scan_id, mx.host, mx.priority, mx.ttl) for mx in get_response(dns_query.mx)]
        )
        cursor.executemany(
            'INSERT INTO record_ns(host_id, scan_id, nameserver_id, ttl) VALUES(?, ?, (SELECT id FROM host WHERE name=?), ?)',
            [(host_id, scan_id, ns.host, ns.ttl) for ns in get_response(dns_query.ns)]
        )
        if dns_query.cname.response:
            cursor.execute(
                'INSERT INTO record_cname(host_id, scan_id, cname, ttl) VALUES(?, ?, ?, ?)',
                (
                    host_id,
                    scan_id,
                    dns_query.cname.response.cname,
                    dns_query.cname.response.ttl
                )
            )
        if dns_query.soa.response:
            cursor.execute(
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
        db.commit()


def get_domains():
    with contextlib.closing(get_db()) as db:
        cursor = db.cursor()
        hosts = [row[0] for row in cursor.execute('SELECT name FROM host WHERE type_id=2')]
        return hosts


def update_domains():
    for query in check_domains(get_domains()):
        save_changes(query)


def main():
    create_db()
    add_domains()
    # save_changes(check_domains(['google.com'])[0])
    # pprint(check_domains(['google.com'])[0])
    # update_domains()

if __name__ == '__main__':
    main()


# TODO add lower and last dot
