import pika


def receive(ch, method, properties, body):
    print(str(body.decode('utf-8')))


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.1.13',
                                                               5672,
                                                               '/',
                                                               credentials))

channel = connection.channel()
channel.basic_consume(
      queue='robots', on_message_callback=receive, auto_ack=True)
channel.start_consuming()
