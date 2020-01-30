import pika
import json
credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.13',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()
channel.queue_declare(queue='bdtables', durable=True)
channel.queue_declare(queue='bdmodule', durable=True)


def resend_tables(ch, method, properties, body):
    print(" [x] Received %r" % body)
    channel.basic_publish(
        exchange='',
        routing_key='bdtables',
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def resend_rfids(ch, method, properties, body):
    print(" [x] Received %r" % body)
    channel.basic_publish(
        exchange='',
        routing_key='bdmodule',
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


channel.basic_consume(
    queue='tables', on_message_callback=resend_tables, auto_ack=True)
channel.basic_consume(
    queue='rfid', on_message_callback=resend_rfids, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
