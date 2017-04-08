import contextlib
from datetime import datetime

import pika

import db
import config


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', type='fanout')

    with contextlib.closing(db.db(config.DB_NAME)) as db_:
        now = datetime.now().timestamp()
        hosts = db.query(db_, db.SELECT_LAST_RECORDS, now, config.BARRIER)
        for host in hosts:
            channel.basic_publish(exchange='logs', routing_key='', body=host[0])
            print(" [x] Sent %r" % host[0])
    connection.close()


if __name__ == '__main__':
    main()
