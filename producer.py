from confluent_kafka import Producer
import time
from datetime import datetime
import random
from threading import Thread

num_of_sensor = 10
start_rnd_sensor_pause = 15 #минимальное значение задержки для сенсора *10
stop_rnd_sensor_pause = 50 #максимальное значение задержки для сенсора *10
types_of_sensor = 4
time_of_work = 70

def get_type(num):
    #Поучение типа дачика
    if num==1:
        return "температура"
    elif num==2:
        return "давление"
    elif num==3:
        return "скорость_ветра"
    else:
        return "влажность"

def create_sensor(num):
    pause = random.randint(start_rnd_sensor_pause, stop_rnd_sensor_pause) / 10
    name = "sensor" + str(num)
    sensor_type = random.randint(1, types_of_sensor)
    return [name, sensor_type, pause]

def send_message(sensor, deadline, TOPIC_NAME):
    while time.monotonic() < deadline:
        message = ' '.join([str(datetime.now()), sensor[0], str(get_type(sensor[1])), str(random.random())])
        producer.produce(TOPIC_NAME, key=bytes(sensor[0] + str(time.monotonic()),  'utf-8'), value=message)
        time.sleep(sensor[2])
        producer.flush()
        print(sensor[0], "Отправил данные")


TOPIC_NAME = "one_topic"

producer = Producer({
        "bootstrap.servers": "localhost:9092"
    })


sensors = [create_sensor(i) for i in range(num_of_sensor)]
deadline = time.monotonic() + time_of_work
threads = []

for num in range(num_of_sensor):
    th = Thread(target=send_message, args=(sensors[num], deadline, TOPIC_NAME, ), name="thread"+str(num))
    th.start()
    threads.append(th)
    producer.flush()


for th in threads:
    th.join()
