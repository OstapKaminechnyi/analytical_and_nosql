import os


class Config(object):
    SECRET_KEY = os.urandom(32)
    APP_FOLDER = 'app/'
    TEMP_FOLDER = ".temp/"
    UPLOAD_FOLDER = APP_FOLDER + TEMP_FOLDER
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)
        os.mknod(UPLOAD_FOLDER+"__init__.py")
    # HEADERS
    CORS_HEADERS = 'Content-Type'
