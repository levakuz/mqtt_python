from PIL import Image
import pika
import numpy as np
import json
from requests import post


credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('95.181.230.223',
                                                               5672,
                                                               '/',
                                                               credentials,
                                                               heartbeat=0,
                                                               blocked_connection_timeout=300
                                                               ))


channel = connection.channel()
channel.queue_declare(queue='robot_map', durable=False)


def get_map(ch, method, properties, body):
    print(json.loads(body))
    new_message = json.loads(body)
    rows = new_message['rows']
    cols = new_message['cols']
    map = np.zeros((int(rows), int(cols)))
    for i in range(0, int(rows)):
        map[i] = new_message[str(i)]
    for i in range(0, int(rows)):
        for j in range(0, int(cols)):
            if map[i][j] == True:
                map[i][j] = 0
            else:
                map[i][j] = 255
    print(map)

    """
    for i in range(0, int(rows)):
        print(i)
        for j in range(0, int(cols)):

            if map[i][j] == 1:
                plt.scatter(i, j, c='black')
    plt.show()"""

    im = Image.fromarray(map)
    im = im.convert('1')
    im = im.transpose(Image.FLIP_TOP_BOTTOM)
    im.show()
    im.save('123.png')
    files = {
        'map': open('123.png', 'rb')
    }
    foo = post('http://95.181.230.223:15032/robot_map', files=files)


channel.basic_consume(on_message_callback=get_map, queue='robot_map', auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()