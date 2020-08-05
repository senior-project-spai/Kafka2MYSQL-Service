import mysql.connector
import json
import time
from uuid import uuid4

from logger import logger
from config import MYSQL_CONFIG_FADE as MYSQL_CONFIG

INSERT_FACE_RECOGNITION_ROW_QUERY = """
INSERT INTO face_recognition
    (id,
     image_id,
     position_top,
     position_right,
     position_bottom,
     position_left,
     label,
     timestamp)
VALUES
    (%(id)s,
     %(image_id)s,
     %(position_top)s,
     %(position_right)s,
     %(position_bottom)s,
     %(position_left)s,
     %(label)s,
     %(timestamp)s);
"""


def handler(msg):
    """ Handler for fade_gender topic """
    # parse string into dict
    msg_dict = json.loads(msg)

    # Open connection
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()

    for _, result in msg_dict["detail"].items():

        params_to_insert = {
            "id": uuid4().hex,
            "image_id": msg_dict['image_id'],
            # position on image
            "position_top": int(result['pos']['y1']),
            "position_right": int(result['pos']['x2']),
            "position_bottom": int(result['pos']['y2']),
            "position_left": int(result['pos']['x1']),
            # label
            "label": result['answer'] if result['answer'] == 'UNKNOWN' else None,
            # epoch in milliseconds
            "timestamp": int(round(time.time() * 1000))
        }

        # Insert into table
        try:
            cursor.execute(INSERT_FACE_RECOGNITION_ROW_QUERY, params_to_insert)
            logger.info(json.dumps(params_to_insert, indent=2))
        except mysql.connector.Error as e:
            logger.error(json.dumps(params_to_insert, indent=2))
            raise e

    # Commit
    database_connection.commit()

    # Close connection
    cursor.close()
    database_connection.close()
