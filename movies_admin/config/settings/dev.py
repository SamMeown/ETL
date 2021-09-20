import os

from .base import *


DEBUG = True

# Для удобной консоли (shell_plus) и пр.
INSTALLED_APPS += ['django_extensions']

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
