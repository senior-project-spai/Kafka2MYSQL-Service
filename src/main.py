from kafka import KafkaConsumer
import mysql.connector

# local module
from logger import logger
from config import KAFKA_HOST, KAFKA_PORT, MYSQL_CONFIG
from topics import age, gender, race, test, object, fade_gender

# create table query
create_age_table_query = ("CREATE TABLE IF NOT EXISTS `Age` (`face_image_id` INT,`min_age` INT,`max_age` INT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")
create_gender_table_query = ("CREATE TABLE IF NOT EXISTS `Gender` (`face_image_id` INT,`type` TEXT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")
create_race_table_query = ("CREATE TABLE IF NOT EXISTS `Race` (`face_image_id` INT,`type` TEXT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")
create_test_table_query = ("CREATE TABLE IF NOT EXISTS `Test` (`face_image_id` INT,`test` INT,`confidence` DOUBLE,`position_top` INT,`position_right` INT,`position_bottom` INT,`position_left` INT,`time` DECIMAL(17,6),`added_time` DECIMAL(17,6),PRIMARY KEY (`face_image_id`),FOREIGN KEY (`face_image_id`) REFERENCES `FaceImage` (`id`));")

# create table query list
add_result_tables = [create_age_table_query,
                     create_gender_table_query,
                     create_race_table_query,
                     create_test_table_query]


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


# Topic handler
topic_handler_dict = {
    'face-result-gender': gender.handler,
    'face-result-race': race.handler,
    'face-result-age': age.handler,
    'face-result-test-service': test.handler,
    'object-result': object.handler,
    'test-gender-result': fade_gender.handler,  # FADE
}

if __name__ == "__main__":
    # Initialize Kafka Consumer
    logger.info("Initialize Kafka Consumer")
    consumer = KafkaConsumer(bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='Kafka2MYSQL-Service-group')
    # Subscribe topics
    consumer.subscribe(topics=['face-result-gender', 'face-result-race',
                               'face-result-age', 'face-result-test-service', 'object-result', 'test-gender-result'])  # FADE

    # Create table if it not exist
    logger.info("Create table if it not exist")
    add_table_to_database()

    # Start consuming messages
    logger.info("Start consuming messages")
    for msg in consumer:
        logger.info(f"NEW Message From [{msg.topic}]")
        topic_handler_dict[msg.topic](msg.value.decode('utf-8'))
