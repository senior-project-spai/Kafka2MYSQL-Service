import mysql.connector
import json
import time
from uuid import uuid4

from logger import logger
from config import MYSQL_CONFIG_FADE as MYSQL_CONFIG
from utils import find_intersect_area

INSERT_AGE_ROW_QUERY = """
INSERT INTO age
    (id,
     image_id,
     position_top,
     position_right,
     position_bottom,
     position_left,
     0_to_10_confidence,
     11_to_20_confidence,
     21_to_30_confidence,
     31_to_40_confidence,
     41_to_50_confidence,
     51_to_60_confidence,
     61_to_70_confidence,
     71_to_100_confidence,
     timestamp)
VALUES
    (%(id)s,
     %(image_id)s,
     %(position_top)s,
     %(position_right)s,
     %(position_bottom)s,
     %(position_left)s,
     %(0_to_10_confidence)s,
     %(11_to_20_confidence)s,
     %(21_to_30_confidence)s,
     %(31_to_40_confidence)s,
     %(41_to_50_confidence)s,
     %(51_to_60_confidence)s,
     %(61_to_70_confidence)s,
     %(71_to_100_confidence)s,
     %(timestamp)s);
"""


def handler(msg):
    """ Handler for fade_age topic """
    # parse string into dict
    msg_dict = json.loads(msg)

    # Open connection
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()

    # Update timestamp when receive the result
    cursor.execute("UPDATE image SET age_timestamp=%(timestamp)s WHERE id=%(id)s;",
                   {'timestamp': int(round(time.time() * 1000)), 'id': msg_dict['image_id']})

    for _, result in msg_dict["detail"].items():

        # Generate ID for each result
        result["_id"] = uuid4().hex

        params_to_insert = {
            "id": result["_id"],
            "image_id": msg_dict['image_id'],
            # position on image
            "position_top": int(result['position']['y1']),
            "position_right": int(result['position']['x2']),
            "position_bottom": int(result['position']['y2']),
            "position_left": int(result['position']['x1']),
            # confidence of each age range
            "0_to_10_confidence": float(result['age_p']['0-10']),
            "11_to_20_confidence": float(result['age_p']['11-20']),
            "21_to_30_confidence": float(result['age_p']['21-30']),
            "31_to_40_confidence": float(result['age_p']['31-40']),
            "41_to_50_confidence": float(result['age_p']['41-50']),
            "51_to_60_confidence": float(result['age_p']['51-60']),
            "61_to_70_confidence": float(result['age_p']['61-70']),
            "71_to_100_confidence": float(result['age_p']['71-100']),
            # epoch in milliseconds
            "timestamp": int(round(time.time() * 1000))
        }

        # Insert into table
        try:
            cursor.execute(INSERT_AGE_ROW_QUERY, params_to_insert)
            logger.info(json.dumps(params_to_insert, indent=2))
        except mysql.connector.Error as e:
            logger.error(json.dumps(params_to_insert, indent=2))
            raise e

    # Commit
    database_connection.commit()
    cursor.close()

    # Close connection
    database_connection.close()
