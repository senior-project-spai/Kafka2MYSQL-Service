import os
import logger

KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_PORT = os.environ['KAFKA_PORT']

MYSQL_HOST = os.environ['MYSQL_MASTER_HOST']
MYSQL_USER = os.environ['MYSQL_MASTER_USER']
MYSQL_PASS = os.environ['MYSQL_MASTER_PASS']
MYSQL_PORT = os.environ['MYSQL_MASTER_PORT']
MYSQL_DB = os.environ['MYSQL_MASTER_DB']

# display environment variable
logger.info('KAFKA_HOST: {}'.format(KAFKA_HOST))
logger.info('KAFKA_PORT: {}'.format(KAFKA_PORT))
logger.info('MYSQL_HOST: {}'.format(MYSQL_HOST))
logger.info('MYSQL_USER: {}'.format(MYSQL_USER))
logger.info('MYSQL_PORT: {}'.format(MYSQL_PORT))
logger.info('MYSQL_DB: {}'.format(MYSQL_DB))