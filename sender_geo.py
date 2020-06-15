import pika
import json

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.13',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()

message = {'key': 'get',
           'cashbox': '',
           'rfid': '',
           'table': '',
           'order': '12',
           'status': '',
           }
channel.basic_publish(
                exchange='',
                routing_key='geo_pos',
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ))


def get_message(ch, method, properties, body):
    """Выполняет пересыл информации номера метки в форму на кассу"""
    print(" [x] Received %r" % body)


channel.basic_consume(
    queue='geo_pos', on_message_callback=get_message, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()



