from pymongo import MongoClient
import pika
import json
import time


def prepare_list(body):
    """Функция создания списка из полученного сообщения"""
    new_message = str(body.decode("utf-8"))
    new_message = new_message.strip()  # Если приходит в виде строки, даныне разделены точкой
    new_message = new_message.lower()
    list_from_message = new_message.split('.')
    return list_from_message


def create_rfidsnums(ch, method, properties, body):
    """Принимает значение RFID uid из RabbitMQ и записывает в БД"""
    print(" [x] Received %r" % body)
    error_2 = "Не получен ключ операции"
    error_1 = "Заказ с таким номером уже существует!"
    one_rfid = json.loads(body)
    print(one_rfid)

    try:
        if one_rfid['key'] == 'EditStatus':
            print(one_rfid['order'])
            print(users.find_one_and_update({'$and': [{'order': one_rfid['order']}, {'table': {'$exists': True}}]},
                                                    {'$set': {'status': one_rfid['status']}}))
            for bd in users.find({}, projection={'_id': False, 'cashbox': False, 'rfid': False}):
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
        elif one_rfid['key'] == 'MakeNew':
            del one_rfid['key']
            print(one_rfid)
            if users.find_one({'order': one_rfid['order'], 'status': {'$ne': 4}}) is None:
                users.insert_one(one_rfid)
                for bd in users.find({}, projection={'_id': False, 'cashbox': False, 'rfid': False}):
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
            else:
                print(error_1)
                channel.basic_publish(
                    exchange='',
                    routing_key='cashboxerrors',
                    body=error_1,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ))
    except IndexError:
        print(error_2)
        channel.basic_publish(
            exchange='',
            routing_key='cashboxerrors',
            body=error_2,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))


def add_tables(ch, method, properties, body):
    """Принимает значение номера стола(table) из RabbitMQ и записывает в БД"""
    print(" [x] Received %r" % body)
    error_1 = "Данный номер метки не найден"
    list_from_message_tables = prepare_list(body)
    print(list_from_message_tables[0])
    for num in numbers.find({'rfid': str(list_from_message_tables[0])}):
        print(num)
    if num is None:
        print(error_1)
        channel.basic_publish(
            exchange='',
            routing_key='tableserrors',
            body=error_1,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    else:
        if users.find_one({'$and': [{'status': {'$ne': '4'}},{'rfid': num['number']}]}) is None:
            print("Ошибка! Нет действующих заказов с такой меткой")
        else:
            users.find_one_and_update({'$and': [{'status': {'$ne': '4'}},{'rfid': num['number']}]},
                                  {'$set': {'table': list_from_message_tables[1]}})
            print("Запись стола успешно обновлена:")
            print(users.find_one({'order': list_from_message_tables[0]}))
            for bd in users.find({}, projection={'_id': False, 'cashbox': False, 'rfid': False}):
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


def check_robot(ch, method, properties, body):
    """Проверка значения метки, полученной роботом """
    print(" [x] Received %r" % body)
    if users.find_one({'$and': [{'status': {"$ne": '4'}}, {'rfid': str(body.decode("utf-8"))}]},
                      projection={'_id': False, 'cashbox': False, 'order': False}) is None:
        channel.basic_publish(
            exchange='',
            routing_key='robots',
            body='False',
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    else:
        channel.basic_publish(
            exchange='',
            routing_key='robots',
            body='True',
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        users.find_one_and_update({'$and': [{'status': '3'}, {'rfid': str(body.decode("utf-8"))}]},
                                  {'$set': {'status': '4'}},
                                  projection={'_id': False, 'cashbox': False, 'order': False})
        for bd in users.find({}, projection={'_id': False, 'cashbox': False, 'rfid': False}):
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


def send_bd(ch, method, properties, body):
    """Отправляет по запросу БД"""
    print(" [x] Received %r" % body)
    for bd in users.find({}, projection={'_id': False,  'cashbox': False, 'rfid': False}):
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


def get_nums(ch, method, properties, body):
    """Выполняет пересыл информации номера метки в форму на кассу"""
    print(" [x] Received %r" % body)
    for num in numbers.find({'rfid': str(body.decode("utf-8"))},
                            projection={'_id': False,  'cashbox': False, 'rfid': False}):
        print(str(num['number']))
    channel.basic_publish(
        exchange='',
        routing_key='rfid',
        body=str(num['number']),
        properties=pika.BasicProperties(
        delivery_mode=2,
        ))


credentials = pika.PlainCredentials('test', 'test')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.97',
                                                               5672,
                                                               '/',
                                                               credentials))
channel = connection.channel()
channel.queue_declare(queue='cashboxerrors', durable=True)
channel.queue_declare(queue='tableserrors', durable=True)
channel.queue_declare(queue='bdmodule', durable=True)
channel.queue_declare(queue='bdtables', durable=True)
channel.queue_declare(queue='bdrobots', durable=True)
channel.queue_declare(queue='robots', durable=True)
channel.queue_declare(queue='rfidnums', durable=True)
channel.queue_declare(queue='GetOrders', durable=True)
channel.queue_declare(queue='orders', durable=True)
mongo_client = MongoClient()
db = mongo_client.new_database
users = db.users
numbers = db.numbers
channel.basic_consume(
        queue='bdmodule', on_message_callback=create_rfidsnums, auto_ack=True)
channel.basic_consume(
        queue='bdtables', on_message_callback=add_tables, auto_ack=True)
channel.basic_consume(
      queue='GetOrders', on_message_callback=send_bd, auto_ack=True)
channel.basic_consume(
      queue='bdrobots', on_message_callback=check_robot, auto_ack=True)
channel.basic_consume(
      queue='rfidnums', on_message_callback=get_nums, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


