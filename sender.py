import pika

HOSTS = ['google.com']


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='logs',
                             type='fanout')

    for host in HOSTS:
        channel.basic_publish(exchange='logs',
                              routing_key='',
                              body=host)
        print(" [x] Sent %r" % host)
    connection.close()


if __name__ == '__main__':
    main()
