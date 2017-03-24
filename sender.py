import contextlib
from datetime import datetime

import pika

import db

BARRIER = 500


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', type='fanout')

    with contextlib.closing(db.get_db()) as db_:
        cursor = db_.cursor()
        now = datetime.now().timestamp()
        hosts = cursor.execute(
            'SELECT host FROM records WHERE abs(last_refresh - ?) > ?',
            (now, BARRIER)
        )
        for host in hosts:
            channel.basic_publish(exchange='logs', routing_key='', body=host[0])
            print(" [x] Sent %r" % host[0])
    connection.close()


if __name__ == '__main__':
    main()
