import pika
import json
credentials = pika.PlainCredentials('lev', 'lev')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.97',
                                                               5672,
                                                               '/',
                                                               credentials))

channel = connection.channel()

channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
