from confluent_kafka import Consumer, KafkaError
import mysql.connector
import time
import os
import json

KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_PORT = os.environ['KAFKA_PORT']

MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASS = os.environ['MYSQL_PASS']
MYSQL_PORT = os.environ['MYSQL_PORT']
MYSQL_DB = os.environ['MYSQL_DB']

c = Consumer({
    'bootstrap.servers': '{}:{}'.format(KAFKA_HOST, KAFKA_PORT),
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['face-result-gender', 'face-result-race', 'test'])

add_gender_query = ("INSERT INTO Gender "
                    "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time) "
                    "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s)")

add_race_query = ("INSERT INTO Race "
                  "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time) "
                  "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s)")


def add_gender(msg):
    msg_json = json.loads(msg)
    print(msg)
    mydb = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASS,
        port=MYSQL_PORT,
        database=MYSQL_DB,
    )
    data_to_update = {
        'face_image_id': msg_json['face_image_id'],
        'type': msg_json['type'],
        'confidence': msg_json['confidence'],
        'position_top': msg_json['position_top'],
        'position_right': msg_json['position_right'],
        'position_bottom': msg_json['position_bottom'],
        'position_left': msg_json['position_left'],
        'time': msg_json['time']
    }
    cursor = mydb.cursor()
    error = False
    try:
        cursor.execute(add_gender_query, data_to_update)
    except (mysql.connector.Error) as e:
        print(e)
        error = True
    mydb.commit()
    cursor.close()
    if not error:
        print("Added")
    mydb.close()
    print()


def add_race(msg):
    msg_json = json.loads(msg)
    print(msg)
    mydb = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASS,
        port=MYSQL_PORT,
        database=MYSQL_DB,
    )
    data_to_update = {
        'face_image_id': msg_json['face_image_id'],
        'type': msg_json['type'],
        'confidence': msg_json['confidence'],
        'position_top': msg_json['position_top'],
        'position_right': msg_json['position_right'],
        'position_bottom': msg_json['position_bottom'],
        'position_left': msg_json['position_left'],
        'time': msg_json['time']
    }
    cursor = mydb.cursor()
    error = False
    try:
        cursor.execute(add_race_query, data_to_update)
    except (mysql.connector.Error) as e:
        print(e)
        error = True
    mydb.commit()
    cursor.close()
    if not error:
        print("Added")
    mydb.close()
    print()


def test(msg):
    msg_json = json.loads(msg)
    print(msg)


func_dict = {'face-result-gender': add_gender,
             'face-result-race': add_race,
             'test': test}

print("SERVICE STARTED MYSQL_HOST:{}, KAFKA_HOST:{}, KAFKA_PORT:{}".format(
    MYSQL_HOST, KAFKA_HOST, KAFKA_PORT))

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print("NEW MSG {}".format(msg.topic()))

    # print('Received message: {}'.format(msg.value().decode('utf-8')))
    func_dict[msg.topic()](msg.value().decode('utf-8'))
c.close()