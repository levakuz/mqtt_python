import paho.mqtt.client as mqtt
import pika
import time

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.13',
                                                               5672,
                                                               '/',
                                                               credentials))


channel = connection.channel()
channel.queue_declare(queue='rfid', durable=True)


def on_message(client , userdata ,  message):
    print("message received ", str(message.payload.decode("utf-8")))
    # nomerok = input('Введите номер номерка: ')
    new_message = str(message.payload.decode("utf-8"))
    channel.basic_publish(
        exchange='',
        routing_key='rfid',
        body=new_message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)


hostIP = "192.168.0.13"
client = mqtt.Client("P2")
client.connect(hostIP)
client.on_message = on_message
client.subscribe("test/")
time.sleep(2)
client.loop_forever()

