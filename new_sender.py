import pika
import json
import paho.mqtt.client as mqtt





credentials = pika.PlainCredentials('lev', 'lev')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.98',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()
while True:
    a = input("Введи статус, номер заказа: ")
    a = a.split(",")
    l = {}
    l.setdefault('status', a[0])
    l.setdefault('order', a[1])
    l.setdefault('rfid', a[1])
    l.setdefault('key', 'MakeNew')
    message = json.dumps(l)
    channel.basic_publish(
        exchange='',
        routing_key='bdmodule',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    if a == '1':
        break
