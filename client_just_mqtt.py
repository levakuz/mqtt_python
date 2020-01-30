import paho.mqtt.client as mqtt
import time


def on_message(client , userdata ,  message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)


hostIP = "192.168.0.13"
#hostIP = "172.20.10.2" #Мобила
#hostIP = "192.168.137.1" #Ноут
client = mqtt.Client("P1")
client.on_message = on_message
client.connect(hostIP)
client.on_message = on_message
client.subscribe("test/")
time.sleep(4)
client.loop_forever()