import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

from time import time

MQTT_HOST = "127.0.0.1"
MQTT_TOPIC = "projeto-sd/"
MQTT_QOS = 2


# mosquitto_sub -h 127.0.0.1 -t projeto-sd/+

# paho.mqtt.python client class example
class mqttClient(mqtt.Client):
    def on_connect(self, mqttc, obj, flags, rc):
        print("rc: " + str(rc))

    def on_connect_fail(self, mqttc, obj):
        print("Connect failed")

    def on_message(self, mqttc, obj, msg):
        print("topic:" + msg.topic + " qos:" + str(msg.qos) + " payload:" + str(msg.payload))

        broken_message = str(msg.payload).split('/')
        version = broken_message[0]
        value = broken_message[1]

        # return version, value

    def on_publish(self, mqttc, obj, mid):
        print("mid: " + str(mid))

    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_log(self, mqttc, obj, level, string):
        # print(string)
        pass

    def run(self):
        self.connect(MQTT_HOST, 1883, 60)
        self.subscribe(MQTT_TOPIC + '#', MQTT_QOS)

        rc = 0
        while rc == 0:
            rc = self.loop()

        return rc

    def sub(self, key):
        self.subscribe(topic=MQTT_TOPIC + str(key),
                       qos=MQTT_QOS)

    def sync_all(self):
        self.subscribe(topic=MQTT_TOPIC + '#',
                       qos=MQTT_QOS)


def pub(key: str, value: str, version: int) -> None:
    try:
        publish.single(topic=MQTT_TOPIC + str(key),
                       payload=str(version) + '/' + str(value),
                       qos=MQTT_QOS)
    except Exception as e:
        raise Exception(str(e))


def dict_message(topic, payload):
    return '...'
