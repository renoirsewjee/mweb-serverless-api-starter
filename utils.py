import sys
import logging
import json
import pendulum
import decimal
import constants
from typing import Dict, Any

# Used to encode data from DynamoDB where numbers are passed back as Decimal
class DecimalEncoder(json.JSONEncoder):
    # pylint: disable=E0202
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def create_logger(name: str):
    console_log_handler = logging.StreamHandler()
    console_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s'))

    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(console_log_handler)

    return logger


def generate_received_timestamp() -> str:
    return pendulum.now(tz=constants.TZ_ZA).to_datetime_string()

        