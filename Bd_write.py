from pymongo import MongoClient
import pika
import json


def prepare_list(body):
    """Функция создания списка из полученного сообщения"""
    new_message = str(body.decode("utf-8"))
    new_message = new_message.strip()  # Если приходит в виде строки, даныне разделены точкой
    new_message = new_message.lower()
    list_from_message = new_message.split('.')
    return list_from_message


def create_one_rfid(list_from_message):
    """Функция создания первоначального словаря, использующегося в последующем для записи в БД """
    one_rfid = {}
    try:
        one_rfid.setdefault('Cashbox', list_from_message[0])
        one_rfid.setdefault('RFID_ID', list_from_message[1])
        one_rfid.setdefault('Order', list_from_message[2])
        one_rfid.setdefault('Status', list_from_message[3])
        one_rfid.setdefault('Table', 0)
    except IndexError:
        print("Значение выходит за границы массива, проверьте информацию")
    return one_rfid


def create_rfidsnums(ch, method, properties, body):
    """Принимает значения из RabbitMQ и записывает в БД"""
    print(" [x] Received %r" % body)

    one_rfid = json.loads(body)
    if one_rfid['key'] == 'EditStatus':
        users.find_one_and_update({'order': one_rfid['order']}, {'$set': {'status': one_rfid['status']}})

    elif one_rfid['key'] == 'MakeNew':
        del one_rfid['key']
        print(one_rfid)
        if users.find_one({'order': one_rfid['order']}) is None:
            users.insert_one(one_rfid)
        else:
            print("Заказ с таким номером уже существует!")
    else:
        print("Не получен ключ операции")

    '''
    if users.find_one({"order": list_from_message[2]}) is None:
        users.insert_one(one_rfid)
        print("Запись успешно проведена: ")
        print(users.find_one({'order': list_from_message[2]}))
    else:
        print("Такой заказ уже существует")
        
       
        channel.basic_publish(
            exchange='',
            routing_key='cashboxerrors',
            body=str("Такой заказ уже существет"),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
            '''

def add_tables(ch, method, properties, body):
    """Принимает значения из RabbitMQ и записывает в БД"""
    print(" [x] Received %r" % body)
    list_from_message_tables = prepare_list(body)
    if users.find_one({'order': list_from_message_tables[0]}) is None:
        print("Данный номер метки не найден")
    else:
        users.find_one_and_update({'order': list_from_message_tables[0]},
                                  {'$set': {'table': list_from_message_tables[1]}})
        print("Запись стола успешно обновлена:")
        print(users.find_one({'order': list_from_message_tables[0]}))



'''
    # Если приходит в виде словаря
    new_rfid ={}
    new_rfid = body
    if rfids.find_one({"RFID": new_rfid['rfid']}) is None:
        rfids.insert_one(new_rfid)
    else:
        rfids.find_one_and_update({"RFID" : new_rfid['rfid']}, {'$set': {'num': new_rfid['order']}})

'''


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.13',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()
channel.queue_declare(queue='cashboxerrors', durable=True)
channel.queue_declare(queue='bdmodule', durable=True)
channel.queue_declare(queue='bdtables', durable=True)
mongo_client = MongoClient()
db = mongo_client.new_database
users = db.users
channel.basic_consume(
        queue='bdmodule', on_message_callback=create_rfidsnums, auto_ack=True)
channel.basic_consume(
        queue='bdtables', on_message_callback=add_tables, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


