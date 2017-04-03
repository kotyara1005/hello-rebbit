# -*- coding: UTF-8 -*-
DEBUG = True
STATIC_FOLDER = None
DB_NAME = 'db.sqlite3'

try:
    from local_settings import *
except ImportError:
    pass
