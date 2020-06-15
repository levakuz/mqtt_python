import paho.mqtt.client as mqtt
import pika
import datetime
from memory_profiler import memory_usage

credentials = pika.PlainCredentials('lev', 'lev')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.98',
                                                               5672,
                                                               '/',
                                                               credentials=credentials,
                                                               # socket_timeout=None,
                                                               # retry_delay=0,
                                                               heartbeat=0,
                                                               blocked_connection_timeout=300,
                                                               ))
channel = connection.channel()
channel.queue_declare(queue='rfidnums', durable=True)
properties = pika.BasicProperties(content_type='text/plain', delivery_mode=1)
#except pika.exceptions.AMQPConnectionError:
    #print("Не удалось подключится к RabbitMq")


def send_to_rabbit(message, queue):
    try:
        channel.basic_publish(
         exchange='',
         routing_key=queue,
         body=message,
         properties=properties,
         )
        print(connection.is_open)
    except:
        print(connection.is_open)
        print("didnt work")


def on_message(client, userdata ,  message):
    if message.topic == "rfids/":
        print("message received ", str(message.payload.decode("utf-8")))
        send_to_rabbit(str(message.payload.decode("utf-8")))
    elif message.topic == "tables/":
        print(datetime.datetime.now())
        new_message = str(message.payload.decode("utf-8"))
        print("message received ", new_message)

        channel.basic_publish(
             exchange='',
             routing_key='bdtables',
             body=new_message
             )
    elif message.topic == "robots/":
        print(datetime.datetime.now())
        new_message = str(message.payload.decode("utf-8"))
        print("message received ", new_message)
        channel.basic_publish(
            exchange='',
            routing_key='bdrobots',
            body=new_message
        )
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)


hostIP = "192.168.1.105"
client = mqtt.Client('P1', clean_session=True)
#client.username_pw_set("lev", "lev")
client.connect(hostIP)
client.on_message = on_message
client.subscribe("rfids/")
client.subscribe("tables/")
client.subscribe("robots/")
print(memory_usage())
client.loop_forever()





