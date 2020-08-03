import mysql.connector
import json

from logger import logger
from config import MYSQL_CONFIG

insert_race_row_query = ("INSERT INTO Race "
                         "(face_image_id, type, confidence, position_top, position_right, position_bottom, position_left, time, added_time) "
                         "VALUES (%(face_image_id)s, %(type)s, %(confidence)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(time)s, unix_timestamp(now(6)))")


def handler(msg):
    # parse json into dict
    msg_json = json.loads(msg)

    # Open connection
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()

    try:
        cursor.execute(insert_race_row_query, {
            'face_image_id': msg_json['face_image_id'],
            'type': msg_json['type'],
            'confidence': msg_json['confidence'],
            'position_top': msg_json['position_top'],
            'position_right': msg_json['position_right'],
            'position_bottom': msg_json['position_bottom'],
            'position_left': msg_json['position_left'],
            'time': msg_json['time']
        })
        database_connection.commit()
        logger.info(msg)
    except (mysql.connector.Error) as e:
        logger.error(e)
        logger.error(msg)

    # Close connection
    cursor.close()
    database_connection.close()
