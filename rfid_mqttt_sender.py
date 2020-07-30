import paho.mqtt.client as mqtt
import pika


credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq',
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
        send_to_rabbit(str(message.payload.decode("utf-8")),'rfidnums')
    elif message.topic == "tables/":
        new_message = str(message.payload.decode("utf-8"))
        print("message received ", new_message)

        channel.basic_publish(
             exchange='',
             routing_key='bdtables',
             body=new_message
             )
    elif message.topic == "robots/":
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


hostIP = 'rabbitmq'
client = mqtt.Client('P1', clean_session=True)
client.connect(hostIP)
client.on_message = on_message
client.subscribe("rfids/")
client.subscribe("tables/")
client.subscribe("robots/")
client.loop_forever()





