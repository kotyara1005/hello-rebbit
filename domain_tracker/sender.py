import json
import contextlib
from datetime import datetime
from itertools import chain

import pika

import db
import config


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    # TODO exchange type
    channel.exchange_declare(exchange='logs', type='fanout')

    with contextlib.closing(db.db(config.DB_NAME)) as database:
        now = datetime.now().timestamp()
        hosts = db.query(
            database,
            db.SELECT_TRACKING_DOMAINS,
            now,
            config.DELAY
        )
        channel.basic_publish(
            exchange='logs',
            routing_key='',
            body=json.dumps(hosts)
        )
        print(" [x] Sent %r" % hosts)
    connection.close()


if __name__ == '__main__':
    main()
