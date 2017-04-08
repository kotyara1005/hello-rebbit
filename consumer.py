import socket
import contextlib
from datetime import datetime

import pika

import db
import config


def resolve(hostname):
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def callback(ch, method, properties, body):
    message = body.decode()
    with contextlib.closing(db.db(config.DB_NAME)) as db_:
        ip = resolve(message)
        now = datetime.now().timestamp()
        db.query(db_, db.UPDATE_RECORDS, ip, now, message)
        db_.commit()
    print('{}|{}'.format(message, ip))


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))

    with connection:
        channel = connection.channel()
        # TODO check RabbitMQ settings
        channel.exchange_declare(exchange='logs', type='fanout')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='logs',
                           queue=queue_name)

        print(' [*] Waiting for logs. To exit press CTRL+C')

        channel.basic_consume(callback,
                              queue=queue_name,
                              no_ack=True)

        channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
