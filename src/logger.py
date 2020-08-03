# log
import logging
logger = logging.getLogger("app")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] - [%(name)s] - [%(levelname)s] - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)