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

func_dict = {"face-result": face_result,  "topic-two": process_topic_two}

c = Consumer({
    'bootstrap.servers': '{}:{}'.format(KAFKA_HOST, KAFKA_PORT),
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['face-result', 'face-result-gender', 'face-result-race'])

add_data_query = ("INSERT INTO data "
        "(epoch, time, gender, gender_confident, race, race_confident, position_top, position_left, position_right, position_bottom, branch_id, camera_id, filepath) "
        "VALUES (%(epoch)s, %(time)s, %(gender)s, %(gender_confident)s, %(race)s, %(race_confident)s, %(position_top)s, %(position_left)s, %(position_right)s, %(position_bottom)s, %(branch_id)s, %(camera_id)s, %(filepath)s)")

def face_result(msg):
    msg_json = json.loads(msg)
    print(msg)
    mydb = mysql.connector.connect(
			host=MYSQL_HOST,
			user=MYSQL_USER,
			passwd=MYSQL_PASS,
			port=MYSQL_PORT,
			database=MYSQL_DB,
    )
    for result in data_json['results']:
        pass
    mydb.close()
	print()

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
