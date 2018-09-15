# -*- coding: utf-8 -*-
# TODO start service task manager
# TODO start service resolve
# TODO start service committer
# TODO start service web face
import asyncio

import click
import aiohttp
import asyncpg

import config
from domain_tracker.lib.utils import Closing
import domain_tracker.task_manager as task_manager_service
import domain_tracker.resolve_service as resolver_service
import domain_tracker.commiter_service as commiter_service


async def _fetch_domains():
    url = 'https://isc.sans.edu/feeds/suspiciousdomains_High.txt'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            domains = [
                line
                for line in (await response.text()).split('\n')
                if not line.startswith('#') and '.' in line
            ]

    async with Closing(asyncpg.connect(**config.DATABASE)) as connection:
        await connection.executemany(
            'INSERT INTO host(name, type_id) VALUES ($1, $2)'
            ' ON CONFLICT (name) DO NOTHING;',
            [(domain, 2) for domain in domains]
        )


async def init_db():
    conn = await asyncpg.connect(**config.DATABASE)
    init_script = open('./domain_tracker/schema.sql').read()
    await conn.execute(init_script)
    await conn.close()


@click.group()
def cli():
    pass


@cli.command()
def initdb():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())


@cli.command()
def fetch_domains():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_fetch_domains())
    click.echo('Domains are successfully fetched')


@cli.command()
def dropdb():
    click.echo('Dropped the database')
    raise NotImplementedError()


@cli.command()
def task_manager():
    task_manager_service.start_service()


@cli.command()
def resolver():
    resolver_service.start_service()


@cli.command()
def committer():
    commiter_service.start_service()


if __name__ == '__main__':
    cli()
