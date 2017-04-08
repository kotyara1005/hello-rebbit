# -*- coding: UTF-8 -*-
from .default_config import *

try:
    from local_settings import *
except ImportError:
    pass
