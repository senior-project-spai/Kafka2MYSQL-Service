from kafka import KafkaProducer
from kafka import KafkaConsumer
import mysql.connector
import time
import os
import json

# log
import logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter(
#     '%(asctime)s - %(name)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_PORT = os.environ['KAFKA_PORT']

MYSQL_HOST = os.environ['MYSQL_MASTER_HOST']
MYSQL_USER = os.environ['MYSQL_MASTER_USER']
MYSQL_PASS = os.environ['MYSQL_MASTER_PASS']
MYSQL_PORT = os.environ['MYSQL_MASTER_PORT']
MYSQL_DB = os.environ['MYSQL_MASTER_DB']

# display environment variable
logger.info('KAFKA_HOST: {}'.format(KAFKA_HOST))
logger.info('KAFKA_PORT: {}'.format(KAFKA_PORT))
logger.info('MYSQL_HOST: {}'.format(MYSQL_HOST))
logger.info('MYSQL_USER: {}'.format(MYSQL_USER))
# logger.info('MYSQL_PASS: {}'.format(MYSQL_PASS))
logger.info('MYSQL_PORT: {}'.format(MYSQL_PORT))
logger.info('MYSQL_DB: {}'.format(MYSQL_DB))

c = KafkaConsumer(['face-result-gender', 'face-result-race',
                   'face-result-age'],
                  bootstrap_servers=[
    '{}:{}'.format(KAFKA_HOST, KAFKA_PORT)],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='Kafka2MYSQL-Service-group')


add_gender_query = ("INSERT INTO Gender "
                    "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                    "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")

add_race_query = ("INSERT INTO Race "
                  "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                  "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")

add_age_query = ("INSERT INTO Age "
                 "(face_image_id, min_age, max_age, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                 "VALUES (%(face_image_id)s, %(min_age)s, %(max_age)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")


def add_gender(msg):
    msg_json = json.loads(msg)
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
        logger.error(e)
        error = True
    mydb.commit()
    cursor.close()
    if not error:
        logger.info(msg)
        logger.info('Added')
    else:
        logger.error(msg)
        logger.error('Error')
    mydb.close()


def add_race(msg):
    msg_json = json.loads(msg)
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
        logger.error(e)
        error = True
    mydb.commit()
    cursor.close()
    if not error:
        logger.info(msg)
        logger.info('Added')
    else:
        logger.error(msg)
        logger.error('Error')
    mydb.close()


def add_age(msg):
    msg_json = json.loads(msg)
    mydb = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASS,
        port=MYSQL_PORT,
        database=MYSQL_DB,
    )
    data_to_update = {
        'face_image_id': msg_json['face_image_id'],
        'min_age': msg_json['min_age'],
        'max_age': msg_json['max_age'],
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
        cursor.execute(add_age_query, data_to_update)
    except (mysql.connector.Error) as e:
        logger.error(e)
        error = True
    mydb.commit()
    cursor.close()
    if not error:
        logger.info(msg)
        logger.info('Added')
    else:
        logger.error(msg)
        logger.error('Error')
    mydb.close()


func_dict = {
    'face-result-gender': add_gender,
    'face-result-race': add_race,
    'face-result-age': add_age
}


logger.info("SERVICE STARTED MYSQL_HOST:{}, KAFKA_HOST:{}, KAFKA_PORT:{}".format(
    MYSQL_HOST, KAFKA_HOST, KAFKA_PORT))

# while True:
#     msg = c.poll(1.0)

#     if msg is None:
#         continue
#     if msg.error():
#         logger.error("Consumer error: {}".format(msg.error()))
#         continue
#     logger.info("NEW Message {}".format(msg.topic()))
#     func_dict[msg.topic()](msg.value().decode('utf-8'))
# c.close()

for msg in c:
    logger.info("NEW Message {}".format(msg.topic))
    func_dict[msg.topic](msg.value.decode('utf-8'))