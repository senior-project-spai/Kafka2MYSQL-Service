import mysql.connector
import json
import time
from uuid import uuid4

from logger import logger
from config import MYSQL_CONFIG_FADE as MYSQL_CONFIG
from utils import find_intersect_area

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
    """ Handler for fade_recognition topic """
    # parse string into dict
    msg_dict = json.loads(msg)

    # Open connection
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()

    for _, result in msg_dict["detail"].items():

        # Generate ID for each result
        result["_id"] = uuid4().hex

        params_to_insert = {
            "id": result["_id"],
            "image_id": msg_dict['image_id'],
            # position on image
            "position_top": int(result['pos']['y1']),
            "position_right": int(result['pos']['x2']),
            "position_bottom": int(result['pos']['y2']),
            "position_left": int(result['pos']['x1']),
            # label
            "label": result['answer'] if result['answer'] != 'UNKNOWN' else None,
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
    cursor.close()

    # Update result to face
    for _, result in msg_dict["detail"].items():

        # Get Dict Cursor
        cursor = database_connection.cursor(dictionary=True)

        # Lock the face table first
        cursor.execute("LOCK TABLE face WRITE;")

        # Find exist face of this image with image_id
        query = ("SELECT id, position_top, position_right, position_bottom, position_left "
                 "FROM face WHERE image_id=%(image_id)s")
        cursor.execute(query, {"image_id": msg_dict['image_id']})
        faces = cursor.fetchall()

        face_to_update = None

        for face in faces:
            # find intersect area between result and face
            area = find_intersect_area({'top': face['position_top'],
                                        'right': face['position_right'],
                                        'bottom': face['position_bottom'],
                                        'left': face['position_left']},
                                       {'top': int(result['pos']['y1']),
                                        'right': int(result['pos']['x2']),
                                        'bottom': int(result['pos']['y2']),
                                        'left': int(result['pos']['x1'])})

            # intersection area threshold
            if area is not None:
                face_to_update = face
                break

        if face_to_update is not None:
            logger.info(f'UPDATE face {face_to_update["id"]}')
            query = ("UPDATE face "
                     "SET face_recognition_id = %(face_recognition_id)s, position_top = %(new_position_top)s, position_right = %(new_position_right)s, position_bottom = %(new_position_bottom)s, position_left = %(new_position_left)s "
                     "WHERE id = %(face_id)s; ")
            cursor.execute(query, {
                'face_id': face_to_update['id'],
                'face_recognition_id': result['_id'],
                'new_position_top': min(face_to_update['position_top'], int(result['pos']['y1'])),
                'new_position_right': max(face_to_update['position_right'], int(result['pos']['x2'])),
                'new_position_bottom': max(face_to_update['position_bottom'], int(result['pos']['y2'])),
                'new_position_left': min(face_to_update['position_left'], int(result['pos']['x1']))
            })
        else:
            # Insert new face if condition is not true
            logger.info('INSERT face')
            query = ("INSERT INTO face (id, image_id, face_recognition_id, position_top, position_right, position_bottom, position_left, timestamp) "
                     "VALUES (%(id)s, %(image_id)s, %(face_recognition_id)s, %(position_top)s, %(position_right)s, %(position_bottom)s, %(position_left)s, %(timestamp)s)")
            cursor.execute(query, {'id': uuid4().hex,
                                   'image_id': msg_dict['image_id'],
                                   'face_recognition_id': result["_id"],
                                   "position_top": int(result['pos']['y1']),
                                   "position_right": int(result['pos']['x2']),
                                   "position_bottom": int(result['pos']['y2']),
                                   "position_left": int(result['pos']['x1']),
                                   "timestamp": int(round(time.time() * 1000)) })

        # Commit
        database_connection.commit()

        # Release lock
        cursor.execute("UNLOCK TABLES;")

        # Close cursor
        cursor.close()

    # Close connection
    database_connection.close()
