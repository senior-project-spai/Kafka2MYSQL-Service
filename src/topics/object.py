import mysql.connector
import json

from logger import logger
from config import MYSQL_CONFIG

insert_object_row_query = ("INSERT INTO `object` (      "
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


def add_object(msg):
    # parse json into dict
    msg_json = json.loads(msg)

    # Open connection
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()

    # For each object in detections
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
            cursor.execute(insert_object_row_query, data_to_update)
            database_connection.commit()
            logger.info(msg)
        except (mysql.connector.Error) as e:
            logger.error(e)
            logger.error(msg)

    # Close connection
    cursor.close()
    database_connection.close()
