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

def main():
	consumer = KafkaConsumer(KAFKA_TOPIC_INPUT,
				    bootstrap_servers=['{}:{}'.format(KAFKA_HOST, KAFKA_PORT)],
				    auto_offset_reset='earliest',
				    enable_auto_commit=True,
				    group_id='my-group',
				    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

	add_data_query = ("INSERT INTO data "
		    "(time, gender, gender_accuracy, race, race_accuracy, position_top, position_left, position_right, position_bottom, branch_id, camera_id, filepath) "
		    "VALUES (%(time)s, %(gender)s, %(gender_accuracy)s, %(race)s, %(race_accuracy)s, %(position_top)s, %(position_left)s, %(position_right)s, %(position_bottom)s, %(branch_id)s, %(camera_id)s, %(filepath)s)")
	print("Kafka2MYSQL-Service Started")
	for data in consumer:
		data_json = data.value
		print('New Data')
		print(data_json)
		mydb = mysql.connector.connect(
			host=MYSQL_HOST,
			user=MYSQL_USER,
			passwd=MYSQL_PASS,
			port=MYSQL_PORT,
			database=MYSQL_DB,
	    )

		for result in data_json['results']:
			data_to_update = {
				'time': data_json['time'],
				'gender':result['gender']['gender'],
				'gender_accuracy':result['gender']['accuracy'],
				'race':result['race']['race'],
				'race_accuracy':result['race']['accuracy'],
				'position_top':result['top'],
				'position_left':result['left'],
				'position_right':result['right'],
				'position_bottom':result['bottom'],
				'branch_id':data_json['branch_id'],
				'camera_id': data_json['camera_id'],
				'filepath':data_json['filepath'],
			}
			cursor = mydb.cursor()
			cursor.execute(add_data_query, data_to_update)
			mydb.commit()
			cursor.close()
			print(str(data_to_update.photo_link)+str(data_to_update.branch_id)+str(data_to_update.camera_id)+" Added")

		mydb.close()
		print()
	
if __name__ == '__main__':
    main()
