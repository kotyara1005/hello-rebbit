import socket
import contextlib
from datetime import datetime

import pika

import db


def resolve(hostname):
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def callback(ch, method, properties, body):
    message = body.decode()
    with contextlib.closing(db.get_db()) as db_:
        cursor = db_.cursor()
        ip = resolve(message)
        now = datetime.now().timestamp()
        cursor.execute(
            'UPDATE records SET ip=?, last_refresh=? WHERE host=?',
            (ip, now, message)
        )
        db_.commit()
    print('{}|{}'.format(message, ip))


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))

    with connection:
        channel = connection.channel()

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
