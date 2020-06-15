from pymongo import MongoClient
import pika
import json

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.13',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()


def send_bd(ch, method, properties, body):
    """Отправляет по запросу БД"""
    print(" [x] Received %r" % body)
    for bd in users.find({'status': {'$ne': 4}}, projection={'_id': False,  'cashbox': False, 'rfid': False}):
        print(bd)
        channel.basic_publish(
            exchange='',
            routing_key='orders',
            body=json.dumps(bd),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    message = json.dumps('end')
    channel.basic_publish(
        exchange='',
        routing_key='orders',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


channel.queue_declare(queue='orders', durable=True)
channel.queue_declare(queue='GetOrders', durable=True)
mongo_client = MongoClient()
db = mongo_client.new_database
users = db.users
channel.basic_consume(
        queue='GetOrders', on_message_callback=send_bd, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
