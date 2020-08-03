from kafka import KafkaConsumer
import mysql.connector
import json

# local module
from logger import logger
from config import KAFKA_HOST, KAFKA_PORT, MYSQL_CONFIG

add_gender_query = ("INSERT INTO Gender "
                    "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                    "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")

add_race_query = ("INSERT INTO Race "
                  "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                  "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")

add_age_query = ("INSERT INTO Age "
                 "(face_image_id, min_age, max_age, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                 "VALUES (%(face_image_id)s, %(min_age)s, %(max_age)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")

add_test_query = ("INSERT INTO Test "
                  "(face_image_id, test, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                  "VALUES (%(face_image_id)s, %(test)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")

add_Age_table = ("CREATE TABLE IF NOT EXISTS `Age` (`face_image_id` INT,`min_age` INT,`max_age` INT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")
add_Gender_table = ("CREATE TABLE IF NOT EXISTS `Gender` (`face_image_id` INT,`type` TEXT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")
add_Race_table = ("CREATE TABLE IF NOT EXISTS `Race` (`face_image_id` INT,`type` TEXT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")
add_Test_table = ("CREATE TABLE IF NOT EXISTS `Test` (`face_image_id` INT,`test` INT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")

add_result_tables = [add_Age_table,
                     add_Gender_table, add_Race_table, add_Test_table]


def add_table_to_database():
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()
    try:
        for table_query in add_result_tables:
            cursor.execute(table_query)
    except (mysql.connector.Error) as e:
        raise e
    database_connection.commit()
    cursor.close()
    database_connection.close()


def add_gender(msg):
    msg_json = json.loads(msg)
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
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
    cursor = database_connection.cursor()
    error = False
    try:
        cursor.execute(add_gender_query, data_to_update)
    except (mysql.connector.Error) as e:
        logger.error(e)
        error = True
    database_connection.commit()
    cursor.close()
    if not error:
        logger.info(msg)
    else:
        logger.error(msg)
    database_connection.close()


def add_race(msg):
    msg_json = json.loads(msg)
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
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
    cursor = database_connection.cursor()
    error = False
    try:
        cursor.execute(add_race_query, data_to_update)
    except (mysql.connector.Error) as e:
        logger.error(e)
        error = True
    database_connection.commit()
    cursor.close()
    if not error:
        logger.info(msg)
    else:
        logger.error(msg)
    database_connection.close()


def add_age(msg):
    msg_json = json.loads(msg)
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
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
    cursor = database_connection.cursor()
    error = False
    try:
        cursor.execute(add_age_query, data_to_update)
    except (mysql.connector.Error) as e:
        logger.error(e)
        error = True
    database_connection.commit()
    cursor.close()
    if not error:
        logger.info(msg)
    else:
        logger.error(msg)
    database_connection.close()


def add_object(msg):
    msg_json = json.loads(msg)
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()
    error = False
    insert_object_query = ("INSERT INTO `object` (      "
                           "    `name`,                 "
                           "    `probability`,          "
                           "    `image_path`,           "
                           "    `position_top`,         "
                           "    `position_right`,       "
                           "    `position_bottom`,      "
                           "    `position_left`         "
                           ")                           "
                           "VALUES (                    "
                           "    %(name)s,               "
                           "    %(probability)s,        "
                           "    %(image_path)s,         "
                           "    %(position_top)s,       "
                           "    %(position_right)s,     "
                           "    %(position_bottom)s,    "
                           "    %(position_left)s       "
                           ")                           ")
    for detection in msg_json["detections"]:
        data_to_update = {
            'name': detection["name"],
            'probability': detection["percentage_probability"] / 100,
            'image_path': msg_json["image_path"],
            'position_top': detection['box_points'][1],
            'position_right': detection['box_points'][2],
            'position_bottom': detection['box_points'][3],
            'position_left': detection['box_points'][0],
        }
        try:
            cursor.execute(insert_object_query, data_to_update)
        except (mysql.connector.Error) as e:
            logger.error(e)
            error = True
    database_connection.commit()
    cursor.close()
    database_connection.close()
    if not error:
        logger.info(msg)
    else:
        logger.error(msg)


def add_test(msg):
    msg_json = json.loads(msg)
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    data_to_update = {
        'face_image_id': msg_json['face_image_id'],
        'test': msg_json['test'],
        'confidence': msg_json['confidence'],
        'position_top': msg_json['position_top'],
        'position_right': msg_json['position_right'],
        'position_bottom': msg_json['position_bottom'],
        'position_left': msg_json['position_left'],
        'time': msg_json['time']
    }
    cursor = database_connection.cursor()
    error = False
    try:
        cursor.execute(add_test_query, data_to_update)
    except (mysql.connector.Error) as e:
        logger.error(e)
        error = True
    database_connection.commit()
    cursor.close()
    if not error:
        logger.info(msg)
    else:
        logger.error(msg)
    database_connection.close()


function_dict = {
    'face-result-gender': add_gender,
    'face-result-race': add_race,
    'face-result-age': add_age,
    'face-result-test-service': add_test,
    'object-result': add_object,
}

if __name__ == "__main__":
    # initialize Kafka Consumer
    consumer = KafkaConsumer(bootstrap_servers=['{}:{}'.format(KAFKA_HOST, KAFKA_PORT)],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='Kafka2MYSQL-Service-group')
    consumer.subscribe(topics=['face-result-gender', 'face-result-race',
                               'face-result-age', 'face-result-test-service', 'object-result'])

    # Create table if it not exist
    add_table_to_database()

    # consume message
    for msg in consumer:
        logger.info("NEW Message {}".format(msg.topic))
        function_dict[msg.topic](msg.value.decode('utf-8'))
