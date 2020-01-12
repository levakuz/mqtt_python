import paho.mqtt.client as mqtt
import time


def on_message(client , userdata ,  message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)


hostIP = "192.168.1.105"
#hostIP = "172.20.10.2" #Мобила
#hostIP = "192.168.137.1" #Ноут
client = mqtt.Client("P1")
client.on_message = on_message
client.connect(hostIP)
client.loop_start()
client.on_message = on_message
client.subscribe("test/")
time.sleep(4)
a = 0
while(True):
    a = input("\nSend message: ")
    if a=='q':
        break
    else:
        client.publish("test/")
        time.sleep(10)
client.loop_stop()