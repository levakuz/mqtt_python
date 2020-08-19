from pymongo import MongoClient
import pika
import json


def refresh_bd_users(routing_key):
    for bd in users.find({}, projection={'_id': False}):

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


def update_robots(routing_key):
    robots_list = []
    for robot in db_robots.find({}, projection={'_id': False}):
        robots_list.append(robot)
    print(robots_list)
    send_message(routing_key, robots_list)


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
            if one_rfid['status'] == '2' or one_rfid['status'] == '3':
                print(users.find_one_and_update({'$and': [{'order': one_rfid['order']}, {'status': {'$ne': '5'}}]},
                                                {'$set': {'status': one_rfid['status']}}))
            elif one_rfid['status'] == '4':
                users.update_one({'$and': [{'order': one_rfid['order']}, {'status': {'$ne': '5'}},
                                                          {'table': {'$exists': True}}]}, {'$set': {'status': '4'}})
            refresh_bd_users("orders")
        elif one_rfid['key'] == 'MakeNew':
            del one_rfid['key']
            print(one_rfid)

            if users.find_one({'rfid': one_rfid['rfid'], 'status': {'$ne': '5'}}) is None:
                users.update_one({'$and': [{'status': {'$ne': '5'}}, {'order': one_rfid['order']}]},
                                {'$set': {'rfid': one_rfid['rfid']}})
            else:
                print(error_3)
                send_message("cashboxerrors", error_3)

            refresh_bd_users('orders')

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
        if users.find_one({'$and': [{'status': {'$ne': '5'}}, {'rfid': num['number']}]}) is None:
            print("Ошибка! Нет действующих заказов с такой меткой")
        else:
            print(list_from_message_tables)
            users.update_one({'$and': [{'status': {'$ne': '5'}}, {'rfid': num['number']}]},
                             {'$set': {'table': list_from_message_tables[1]}})
            print("Запись стола успешно обновлена:")
            print(users.find_one({'order': list_from_message_tables[0]}))
            for user in users.find({'$and': [{'status': {'$ne': '5'}}, {'rfid': num['number']}]},
                                   projection={'_id': False, 'rfid': False, 'cashbox': False, 'status': False}):
                send_message('update_tables', json.dumps(user))


def check_robot(ch, method, properties, body):
    """Проверка значения метки, полученной роботом """
    print(" [x] Received %r" % body)
    for num in numbers.find({'rfid': str(body.decode("utf-8"))}):
        print(num['number'])

    for user in users.find({'$and': [{'status': '4'}, {'rfid': num['number']}, {'robot_id': num['robot_id']}]},
                           projection={'_id': False, 'cashbox': False}):
        print(user)
        if user is None:
            send_message('ROSINFO', 'False')
        else:
            send_message('ROSINFO', 'True')
            users.find_one_and_update({'$and': [{'status': '4'}, {'rfid': num['number']}, {'robot_id': num['robot_id']}]}, {'$set': {'status': '5'}})
            for updated_user in users.find({'$and': [{'order': user['order']}, {'rfid': num['number']}]},
                                           projection={'_id': False, 'rfid': False, 'cashbox': False, 'table': False}):
                send_message('update_robot', json.dumps(updated_user))


def get_nums(ch, method, properties, body):
    """Выполняет пересыл информации номера метки в форму на кассу"""
    print(" [x] Received %r" % body)
    for num in numbers.find({'rfid': str(body.decode("utf-8"))},
                            projection={'_id': False}):
        print(num)
        send_message('rfid', json.dumps(num))


def get_bd_request(ch, method, properties, body):
    """Прием запроса на отправку БД"""
    print(" [x] Received %r" % body)
    refresh_bd_users('orders')


def get_parsing_orders(ch, method, properties, body):
    """Создает новые спарщенные заказы из полученного массива"""
    print(json.loads(body))
    print(body)
    for j in json.loads(body):
        if users.find_one({'order': j['order']}) is None:
            users.insert_one(j)
        else:
            users.find_one_and_update({'$and': [{'order': j['order']}, {'status': {'$lt': '4'}}]},
                                      {'$set': {'status': j['status']}})

    refresh_bd_users('orders')


def clear_data(ch, method, properties, body):
    users.remove()


def order_data_geopos_gui(ch, method, properties, body):
    order_list = []
    print(body)
    number = json.loads(body)
    print(number['key'])
    if str(number['key']) == "1":
        """Отправляю данные о всех заказах"""
        for user in users.find({'status': {'$ne': '5'}},   # Ищу все активные заказы
                               projection={'_id': False}):
            order_list.append(user)
        print(order_list)
        ch.basic_publish(exchange='',                       # Отправка всех активных заказов в очередь ответа
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                         properties.correlation_id),
                         body=json.dumps(order_list))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    elif str(number['key']) == "2":

        """Отправляю данные о конкретном заказе"""
        print(number['order'])
        for user in users.find({'$and': [{'status': {'$ne': '5'}}, {'order': number['order']}]},
                               projection={'_id': False, 'cashbox': False}):
            order_list.append(user)
        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=\
                                                                 properties.correlation_id),
                         body=json.dumps(order_list))
        ch.basic_ack(delivery_tag=method.delivery_tag)


def robot_db_response(ch, method, properties, body):
    update_robots('robots')


def robot_interface_message(ch, method, properties, body):
    print(body)
    new_robot_data_list = json.loads(body)
    print(new_robot_data_list)
    print(len(new_robot_data_list))
    for i in range(len(new_robot_data_list)):
        print(new_robot_data_list[i]['id'])
        users.update_one({'$and': [{'order': new_robot_data_list[i]['id']}, {'robot_id': {'$exists': False}},
                                   {'table': {'$exists': True}}, {'status': '3'}]},
                         {'$set': {'robot_id': new_robot_data_list[i]['robot']}})
        users.update_one({'$and': [{'order': new_robot_data_list[i]['id']}, {'robot_id': {'$exists': True}}]},
                         {'$set': {'status': '4'}})
        refresh_bd_users('orders')
    for user in users.find({'$and': [{'order': new_robot_data_list[i]['id']}, {'robot_id': {'$exists': True}}]}):
        print(user)
        send_message('ros_delivery_table', user['table'])


def update_robot_status(ch, method, properties, body):
    print(body)
    new_robot_data = json.loads(body)
    print(new_robot_data)
    db_robots.find_one_and_update({'robot_id': new_robot_data}, {'$set': {'is_active': 1}})
    update_robots('robots')


def robot_db_response_user(ch, method, properties, body):
    robots_list = []
    robot_info = json.loads(body)
    for user in db_robots.find({}, projection={'_id': False}):
        robots_list.append(user)
    ch.basic_publish(exchange='',
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                     properties.correlation_id),
                     body=json.dumps(robots_list))


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
channel.queue_declare(queue='parser_clear_data', durable=False)
channel.queue_declare(queue='parser_data', durable=False)
channel.queue_declare(queue='rpc_robots_db', durable=False)
channel.queue_declare(queue='set_selected_orders', durable=False)
channel.queue_declare(queue='set_robot_status', durable=False)
channel.queue_declare(queue='get_robots', durable=False)
channel.queue_declare(queue='rpc_find_order_for_interface', durable=False)
channel.queue_declare(queue='ros_delivery_table', durable=False)

mongo_client = MongoClient('192.168.0.17', 2717)
db = mongo_client.new_database
users = db.users
numbers = db.numbers
db_robots = db.robots

channel.basic_consume(on_message_callback=robot_db_response, queue='get_robots', auto_ack=True)
channel.basic_consume(on_message_callback=order_data_geopos_gui, queue='rpc_find_order_for_interface', auto_ack=True)
channel.basic_consume(on_message_callback=robot_db_response_user, queue='rpc_robots_db', auto_ack=True)
channel.basic_consume(on_message_callback=robot_interface_message, queue='set_selected_orders', auto_ack=True)
channel.basic_consume(on_message_callback=update_robot_status, queue='set_robot_status', auto_ack=True)
channel.basic_consume(queue='bdmodule', on_message_callback=create_rfidsnums, auto_ack=True)
channel.basic_consume(queue='bdtables', on_message_callback=add_tables, auto_ack=True)
channel.basic_consume(queue='GetOrders', on_message_callback=get_bd_request, auto_ack=True)
channel.basic_consume(queue='bdrobots', on_message_callback=check_robot, auto_ack=True)
channel.basic_consume(queue='rfidnums', on_message_callback=get_nums, auto_ack=True)
channel.basic_consume(queue='parser_data', on_message_callback=get_parsing_orders, auto_ack=True)
channel.basic_consume(queue='parser_clear_data', on_message_callback=clear_data, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()