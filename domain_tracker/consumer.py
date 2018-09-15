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
    now = datetime.now().timestamp()
    for domain_id in message:
        with contextlib.closing(db.db(config.DB_NAME)) as db_:
            resolved_records = resolve(message)
            db_records = db.query(db_, db.GET_RECORDS_A, domain_id)
            new_records = {
                record.host: record
                for record in resolved_records
                if record.host not in (r[1] for r in db_records)
            }
            db.query(
                db_,
                'INSERT INTO records_a VALUES (domain_id, ip_address_id)'
            )
            update_records = {
                record.host: record
                for record in resolved_records
                if record.host in (r[1] for r in db_records)
            }
            db.query(
                db_,
                'UPDATE records_a SET '
            )
            close_records = {
                record[1]: record
                for record in db_records
                if record[1] not in (r.host for r in resolved_records)
            }
            for record in close_records.values():
                db.query(
                    db_,
                    'UPDATE records_a SET outdated_at = ? WHERE id=?',
                    now,
                    record[0]
                )
            # TODO commit
            # TODO clean data
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
