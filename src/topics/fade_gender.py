import mysql.connector
import json

from logger import logger
from config import MYSQL_CONFIG

insert_gender_row_query = """
INSERT INTO Gender
    (image_id,
     type,
     confidence,
     position_top,
     position_right,
     position_bottom,
     position_left)
VALUES
    (%(image_id)s,
     %(type)s,
     %(confidence)s,
     %(position_top)s,
     %(position_right)s,
     %(position_bottom)s,
     %(position_left)s);
"""


def handler(msg):
    # parse string into dict
    msg_dict = json.loads(msg)

    # Open connection
    database_connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = database_connection.cursor()

    for index, face in msg_dict["detail"].items():
        print(index, face)

        # TODO: params

        # TODO: Insert into table
        # 

    # Commit
    database_connection.commit()

    # Close connection
    cursor.close()
    database_connection.close()