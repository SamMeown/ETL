from .base import *
import os


DEBUG = True

# Настройки для debug_toolbar
USE_DEBUG_TOOLBAR = False
if USE_DEBUG_TOOLBAR:
    INSTALLED_APPS += [
        'debug_toolbar',
    ]

    MIDDLEWARE += ['debug_toolbar.middleware.DebugToolbarMiddleware']

    STATIC_ROOT = os.path.join(BASE_DIR, 'static/')

    INTERNAL_IPS = [
        '127.0.0.1',
    ]
