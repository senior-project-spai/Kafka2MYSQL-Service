import os
from logger import logger

KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_PORT = os.environ['KAFKA_PORT']

MYSQL_CONFIG = {
    "host":  os.environ['MYSQL_MASTER_HOST'],
    "user": os.environ['MYSQL_MASTER_USER'],
    "passwd": os.environ['MYSQL_MASTER_PASS'],
    "port": os.environ['MYSQL_MASTER_PORT'],
    "database": os.environ['MYSQL_MASTER_DB']
}

# display environment variable
logger.info(f'KAFKA_HOST_PORT: {KAFKA_HOST}:{KAFKA_PORT}')
logger.info(f'MYSQL_CONFIG: {MYSQL_CONFIG}')