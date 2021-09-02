from .base import *
import os


DEBUG = True

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('DB_NAME'),
        'USER': os.environ.get('DB_USER'),
        'PASSWORD': os.environ.get('DB_PASSWORD'),
        'HOST': os.environ.get('DB_HOST', '127.0.0.1'),
        'PORT': os.environ.get('DB_PORT', 5432),
        'OPTIONS': {
            'options': '-c search_path=public,content'
        }
    }
}

# Настройки для debug_toolbar
#
# INSTALLED_APPS += [
#     'debug_toolbar',
# ]
#
# MIDDLEWARE += ['debug_toolbar.middleware.DebugToolbarMiddleware']
#
# STATIC_ROOT = os.path.join(BASE_DIR, 'static/')
#
# INTERNAL_IPS = [
#     # ...
#     '127.0.0.1',
#     # ...
# ]
