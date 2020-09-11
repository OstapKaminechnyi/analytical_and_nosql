from flask import Flask
from os import environ
from flask_cors import CORS, cross_origin

from config import Config


app = Flask(__name__)
app.config.from_object(Config)
cors = CORS(app)

import logging
app.logger.setLevel(logging.INFO)

from app import loggers
from app import constants
from app import views
from app import redis_init
# from app.event_hub import sender, receiver
