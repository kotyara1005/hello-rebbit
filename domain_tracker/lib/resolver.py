from datetime import datetime
from collections import namedtuple

import aiodns


DnsQuery = namedtuple('DnsRequest',
                      'host created_at a, aaaa, cname, mx, ns, soa')
DnsResponse = namedtuple('DnsResponse', 'response status')


class Resolver:
    def __init__(self, *, nameservers=None, loop=None):
        self._resolver = aiodns.DNSResolver(nameservers=nameservers, loop=loop)

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
