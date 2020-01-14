from confluent_kafka import Consumer, KafkaError
import mysql.connector
import time
import os
import json
KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_PORT = os.environ['KAFKA_PORT']
KAFKA_TOPIC_INPUT = os.environ['KAFKA_TOPIC_INPUT']

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

add_gender_query = ("INSERT INTO gender "
                    "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time) "
                    "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s)")

add_race_query = ("INSERT INTO race "
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
    cursor.execute(add_gender_query, data_to_update)
    mydb.commit()
    cursor.close()
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
    cursor.execute(add_race_query, data_to_update)
    mydb.commit()
    cursor.close()
    print("Added")
    mydb.close()
    print()


def test(msg):
    msg_json = json.loads(msg)
    print(msg)


func_dict = {'face-result-gender': add_gender,
             'face-result-race': add_race,
             'test': test}

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    # print('Received message: {}'.format(msg.value().decode('utf-8')))
    func_dict[msg.topic()](msg.value().decode('utf-8'))
c.close()
