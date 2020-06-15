import pika
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
import gridfs
import base64
import datetime
mongo_client = MongoClient()
db = mongo_client.new_database
dbmap = db.map
new_map = ""
Row = 0
i = 0
#file_db = fs.new_file()
#file_contain = open('map.txt', 'wb')


def consume(ch, method, properties, body):
    print(" [x] Received %r" % body)
    message = str(body.decode("utf-8"))
    global new_map
    global i
    """Принимает значение номера стола(table) из RabbitMQ и записывает в БД"""
    """
    file_contain = open('map.txt', 'ab')
    print(" [x] Received %r" % body)
    message = str(body.decode("utf-8"))

    if len(message) == 1:
        file_contain.write(message)
       
    elif message == 'end':
        file_contain.close()
        new_map = open('map.txt', 'rb')
        i = 1
        fs.delete("1")
        grid_in = fs.upload_from_stream_with_id(
            "1",
            "map",
            new_map,
            chunk_size_bytes=4,
            metadata={"contentType": "text/plain"})
        file_contain = open('map.txt', 'w')
        file_contain.close()
"""


    if len(message) == 1:

        new_map += message

    elif message == 'end':
        map_file_bytes = new_map.encode("utf-8")
        map_file = base64.b64encode(map_file_bytes)
        now = datetime.datetime.now()
        time = str(str(now.hour) + ":" + str(now.minute) + ":" + str(now.second))
        date = str(str(now.day) + "." + str(now.month) + "." + str(now.year))
        dbmap.update_one({'name': 'map'}, {'$set': {'data': map_file,
                                                    'date': date,
                                                    'time': time}})
        new_map = ""


    """
    if len(message) > 1 and message != 'end':
        global i
        i = 0
        list_from_message = message.split(".")              #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        global Row                                          #!!!!!!Блок кода для построения напрямую в питоне!!!!!!!!!
        Row = int(list_from_message[1])                     #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!       
        print(Row)

    elif len(message) == 1:
        global map

        map[Row, i] = int(message)
        i = i + 1

    elif message == 'end':
        print('done')
        plt.xlim(0, map.shape[0])
        plt.ylim(0, map.shape[0])
        for idx in range(0, map.shape[0]):
             for j in range(0, map.shape[0]):
                 if map[idx, j] == 0:
                       plt.scatter(idx, j, alpha=0.8, c='gray',
                              marker='.')
        plt.show()
"""




credentials = pika.PlainCredentials('user', 'user')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.97',
                                                               5672,
                                                               '/',
                                                               credentials))

channel = connection.channel()
channel.queue_declare(queue='map', durable=False)
channel.basic_consume(
        queue='map', on_message_callback=consume, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
 
channel.start_consuming()