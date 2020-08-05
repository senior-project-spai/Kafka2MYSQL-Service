import mysql.connector
import json
import time
from uuid import uuid4

from logger import logger
from config import MYSQL_CONFIG_FADE as MYSQL_CONFIG

INSERT_EMOTION_ROW_QUERY = """
INSERT INTO emotion
    (id,
     image_id,
     position_top,
     position_right,
     position_bottom,
     position_left,
     uncertain_confidence,
     angry_confidence,
     disgusted_confidence,
     fearful_confidence,
     happy_confidence,
     neutral_confidence,
     sad_confidence,
     surprised_confidence,
     timestamp)
VALUES
    (%(id)s,
     %(image_id)s,
     %(position_top)s,
     %(position_right)s,
     %(position_bottom)s,
     %(position_left)s,
     %(uncertain_confidence)s,
     %(angry_confidence)s,
     %(disgusted_confidence)s,
     %(fearful_confidence)s,
     %(happy_confidence)s,
     %(neutral_confidence)s,
     %(sad_confidence)s,
     %(surprised_confidence)s,
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
            "position_top": int(result['position']['y1']),
            "position_right": int(result['position']['x2']),
            "position_bottom": int(result['position']['y2']),
            "position_left": int(result['position']['x1']),
            # confidence of each type
            "uncertain_confidence": float(result['emotion_p']['Uncertain']),
            "angry_confidence": float(result['emotion_p']['angry']),
            "disgusted_confidence": float(result['emotion_p']['disgusted']),
            "fearful_confidence": float(result['emotion_p']['fearful']),
            "happy_confidence": float(result['emotion_p']['happy']),
            "neutral_confidence": float(result['emotion_p']['neutral']),
            "sad_confidence": float(result['emotion_p']['sad']),
            "surprised_confidence": float(result['emotion_p']['surprised']),
            # epoch in milliseconds
            "timestamp": int(round(time.time() * 1000))
        }

        # Insert into table
        try:
            cursor.execute(INSERT_EMOTION_ROW_QUERY, params_to_insert)
            logger.info(json.dumps(params_to_insert, indent=2))
        except mysql.connector.Error as e:
            logger.error(json.dumps(params_to_insert, indent=2))
            raise e

    # Commit
    database_connection.commit()

    # Close connection
    cursor.close()
    database_connection.close()
