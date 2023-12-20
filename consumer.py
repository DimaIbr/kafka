from confluent_kafka import Consumer

import time
import pandas as pd

time_of_work = 70
time_collecting_data = 20

def mes_to_df(sensor_dict):
    #Переброс коллекции сообщений из словаря в DataFrame, берёт среднее значение
    srez = {}
    for i in sensor_dict:
        summ = 0
        for j in sensor_dict[i]:
            summ += float(j)
        srez[i] = summ / len(sensor_dict[i])
    df_name = pd.DataFrame.from_dict(data=srez, orient="index", columns=["value"])
    return df_name

TOPIC_NAME = "one_topic"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "group1",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC_NAME])

sensor_name_dict = {}
sensor_type_dict = {}
start_time = time.monotonic()
last_update_time = time.monotonic()
deadline = start_time + time_of_work
while time.monotonic() < deadline:
    #Чтобы работал только заданное время time_of_work
    msg = consumer.consume(num_messages=1, timeout=1.0)
    if len(msg) > 0:
        mes = str(msg[0].value(), encoding='utf-8').split()
        if mes[2] in sensor_name_dict:
            sensor_name_dict[mes[2]].append(mes[4])
        else:
            sensor_name_dict[mes[2]] = [mes[4]]
        if mes[3] in sensor_type_dict:
            sensor_type_dict[mes[3]].append(mes[4])
        else:
            sensor_type_dict[mes[3]] = [mes[4]]
    if time.monotonic()-last_update_time >= time_collecting_data:
        #Обновление и вывод датафреймов
        last_update_time = time.monotonic()
        df_name = mes_to_df(sensor_name_dict)
        sensor_name_dict = {}
        print(df_name)
        df_type = mes_to_df(sensor_type_dict)
        sensor_type_dict = {}
        print(df_type)

consumer.close()