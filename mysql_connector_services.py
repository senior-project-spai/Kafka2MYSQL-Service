import mysql.connector
import time
from kafka import KafkaConsumer
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

consumer = KafkaConsumer(KAFKA_TOPIC_INPUT,
                            bootstrap_servers=['{}:{}'.format(KAFKA_HOST, KAFKA_PORT)],
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            group_id='my-group',
			    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

add_data_query = ("INSERT INTO data "
            "(time, Gender, Race, position_top, position_left, position_right, position_bottom) "
            "VALUES (%(time)s, %(Gender)s, %(Race)s, %(position_top)s, %(position_left)s, %(position_right)s, %(position_bottom)s)")

for data in consumer:
    mydb = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASS,
        port=MYSQL_PORT,
        database=MYSQL_DB,
    )
    cursor = mydb.cursor()
    print('New Data')
    print(data.value)
    cursor.execute(add_data_query, data.value)
    mydb.commit()
    cursor.close()
    mydb.close()
    print('Added to MYSQL')
    print()
