# -*- coding: utf-8 -*-
# pylint: disable=locally-disabled, multiple-statements
# pylint: disable=fixme, line-too-long, invalid-name
# pylint: disable=W0703

import sys
from os import environ, path
from environs import Env


ENV_FILE = path.join(path.abspath(path.dirname(__file__)), '.env')

try:
    ENVIR = Env()
    ENVIR.read_env()
except Exception as e:
    print('Error: .env file not found: %s' % e)
    sys.exit(1)


class Config:
    """
    This is the generic loader that sets common attributes

    :param: None
    :return: None
    """

    if environ.get('MQTT_HOST'):
        MQTT_HOST = ENVIR('MQTT_HOST')

    if environ.get('MQTT_PORT'):
        MQTT_PORT = int(ENVIR('MQTT_PORT'))

    if environ.get('MQTT_TOPIC_IN'):
        MQTT_TOPIC_IN = ENVIR('MQTT_TOPIC_IN')

    if environ.get('DB_HOST'):
        DB_HOST = ENVIR('DB_HOST')

    if environ.get('DB_USER'):
        DB_USER = ENVIR('DB_USER')

    if environ.get('DB_NAME'):
        DB_NAME = ENVIR('DB_NAME')

    if environ.get('DB_PASS'):
        DB_PASS = ENVIR('DB_PASS')