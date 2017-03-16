import socket

import pika


def resolve(hostname):
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def callback(ch, method, properties, body):
    message = body.decode()
    print('{}|{}'.format(message, resolve(message)))


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
