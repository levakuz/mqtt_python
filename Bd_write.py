from pymongo import MongoClient
import pika
import json


def refresh_bd_users(routing_key):
    for bd in users.find({}, projection={'_id': False, 'cashbox': False, 'rfid': False}):
        print(bd)
        channel.basic_publish(
            exchange='',
            routing_key=routing_key,
            body=json.dumps(bd),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    message = json.dumps('end')
    channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def send_message(routing_key, message):
    channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


def send_bd(routing_key, message):
    channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))

    message = json.dumps('end')
    channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))


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
    error_3 = "Заказ с такой меткой существует!"
    one_rfid = json.loads(body)
    print(one_rfid)
    try:
        if one_rfid['key'] == 'EditStatus':
            print(one_rfid['order'])
            if one_rfid['status'] == '2':
                print(users.find_one_and_update({'$and': [{'order': one_rfid['order']}, {'status': {'$ne': '4'}}]},
                                                {'$set': {'status': one_rfid['status']}}))
            elif one_rfid['status'] == '3':
                print(users.find_one_and_update({'$and': [{'order': one_rfid['order']}, {'status': {'$ne': '4'}},
                                                          {'table': {'$exists': True}}]}, {'$set': {'status': '3'}}))
                for user in users.find({'order': one_rfid['order']}, projection={'_id': False}):
                    print(user)
                channel.basic_publish(
                    exchange='',
                    routing_key='ROSINFO',
                    body=user['table'],
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ))

            refresh_bd_users("orders")
        elif one_rfid['key'] == 'MakeNew':
            del one_rfid['key']
            print(one_rfid)
            if users.find_one({'order': one_rfid['order'], 'status': {'$ne': '4'}}) is None:
                if users.find_one({'rfid': one_rfid['rfid'], 'status': {'$ne': '4'}}) is None:
                    users.insert_one(one_rfid)
                else:
                    print(error_3)
                    send_message("cashboxerrors", error_3)
                refresh_bd_users('orders')
            else:
                send_message("cashboxerrors", error_1)
                print(error_1)
    except IndexError:
        print(error_2)
        send_message("cashboxerrors", error_2)


def add_tables(ch, method, properties, body):
    """Принимает значение номера стола(table) из RabbitMQ и записывает в БД"""
    print(" [x] Received %r" % body)
    error_1 = "Данный номер метки не найден"
    list_from_message_tables = prepare_list(body)
    print(list_from_message_tables[0])
    for num in numbers.find({'rfid': str(list_from_message_tables[0])}):
        print(num['number'])
    if num is None:
        print(error_1)
        send_message("tableserrors", error_1)
    else:
        if users.find_one({'$and': [{'status': {'$ne': '4'}}, {'rfid': num['number']}]}) is None:
            print("Ошибка! Нет действующих заказов с такой меткой")
        else:
            print(list_from_message_tables)
            users.update_one({'$and': [{'status': {'$ne': '4'}}, {'rfid': num['number']}]},
                             {'$set': {'table': list_from_message_tables[1]}})
            print("Запись стола успешно обновлена:")
            print(users.find_one({'order': list_from_message_tables[0]}))
            refresh_bd_users('orders')


def check_robot(ch, method, properties, body):
    """Проверка значения метки, полученной роботом """
    print(" [x] Received %r" % body)
    for num in numbers.find({'rfid': str(body.decode("utf-8"))}):
        print(num['number'])
    if users.find_one({'$and': [{'status': {"$ne": '4'}}, {'rfid': num['number']}]},
                      projection={'_id': False, 'cashbox': False, 'order': False}) is None:
        channel.basic_publish(
            exchange='',
            routing_key='ROSINFO',
            body='False',
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
    else:
        channel.basic_publish(
            exchange='',
            routing_key='ROSINFO',
            body='True',
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        users.find_one_and_update({'$and': [{'status': '3'}, {'rfid': num['number']}]},
                                  {'$set': {'status': '4'}})
        refresh_bd_users('orders')


def get_nums(ch, method, properties, body):
    """Выполняет пересыл информации номера метки в форму на кассу"""
    print(" [x] Received %r" % body)
    for num in numbers.find({'rfid': str(body.decode("utf-8"))},
                            projection={'_id': False}):
        print(num)
        channel.basic_publish(
            exchange='',
            routing_key='rfid',
            body=json.dumps(num),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))


def get_bd_request(ch, method, properties, body):
    """Прием запроса на отправку БД"""
    print(" [x] Received %r" % body)
    refresh_bd_users('orders')


credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.0.17',
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
channel.queue_declare(queue='ROSINFO', durable=False)
mongo_client = MongoClient('192.168.0.17', 2717)
db = mongo_client.new_database
users = db.users
numbers = db.numbers
geo_positions = db.geopositions
channel.basic_consume(
    queue='bdmodule', on_message_callback=create_rfidsnums, auto_ack=True)
channel.basic_consume(
    queue='bdtables', on_message_callback=add_tables, auto_ack=True)
channel.basic_consume(
    queue='GetOrders', on_message_callback=get_bd_request, auto_ack=True)
channel.basic_consume(
    queue='bdrobots', on_message_callback=check_robot, auto_ack=True)
channel.basic_consume(
    queue='rfidnums', on_message_callback=get_nums, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()




