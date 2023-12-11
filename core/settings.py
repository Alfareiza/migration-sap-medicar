"""
Django settings for core project.

Generated by 'django-admin startproject' using Django 4.2.2.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.2/ref/settings/
"""
import logging
from functools import partial
from os.path import join
from pathlib import Path

from decouple import config
from dj_database_url import parse

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = config('SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = config("DEBUG", cast=bool)

ALLOWED_HOSTS = ['*']

# Application definition
INSTALLED_APPS = [
    # 'django.contrib.admin',
    # 'django.contrib.auth',
    # 'django.contrib.contenttypes',
    # 'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.humanize',
    'base'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    # 'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    # 'django.contrib.auth.middleware.AuthenticationMiddleware',
    # 'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'core.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'core.wsgi.application'

# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases
default_db_url = 'sqlite:///' + join(BASE_DIR, 'db.sqlite3')
parse_database = partial(parse, conn_max_age=600)
DATABASES = {
    'default': config('DATABASE_URL', default=default_db_url,
                      cast=parse_database)
}

# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = 'es-co'

TIME_ZONE = 'America/Bogota'

USE_I18N = True

USE_TZ = False

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = 'static/'
MEDIA_ROOT = BASE_DIR / 'tmp'

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Admins | Error Reporting https://docs.djangoproject.com/en/4.1/howto/error-reporting/
ADMINS = [('Alfonso AG', 'alfareiza@gmail.com')]

# Email Configuration

EMAIL_BACKEND = config('EMAIL_BACKEND')
EMAIL_HOST = config('EMAIL_HOST')
EMAIL_PORT = config('EMAIL_PORT')
EMAIL_HOST_USER = config('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = config('EMAIL_HOST_PASSWORD')
EMAIL_USE_SSL = config('EMAIL_USE_SSL')

# create logger
logger = logging.getLogger("logging_tryout2")
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter("%(asctime)s %(funcName)s %(levelname)s %(message)s",
                              "[%d/%b/%Y %H:%M:%S]")
# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

# HEADERS CSV

DISPENSACION_HEADER = {
    'FechaDispensacion', 'SubPlan', 'NIT', 'Plan', 'NroDocumento',
    'Beneficiario', 'NroSSC', 'Categoria', 'NroAutorizacion',
    'Mipres', 'UsuarioDispensa', 'Plu', 'CECO', 'Lote', 'CantidadDispensada',
    # 'Vlr.Unitario Margen'
    'Precio'
}

FACTURACION_HEADER = {
    'FechaFactura', 'NIT', 'Plan', 'SubPlan', 'Factura', 'NroDocumento',
    'Beneficiario', 'NroSSC', 'Categoria', 'NroAutorizacion',
    'Mipres', 'Plu', 'CECO', 'CantidadDispensada', 'Precio'
}

NOTAS_CREDITO_HEADER = {
    'FechaFactura', 'NIT', 'Plan', 'NroSSC',
    'Beneficiario', 'CategoriaActual', 'NroAutorizacion',
    'MiPres', 'Plu', 'CECO', 'CantidadDispensada',
    'Precio', 'Lote', 'UsuarioDispensa'
}

AJUSTES_SALIDA_HEADER = {
    'FechaAjuste', 'Plu', 'TipoAjuste', 'CECO',
    'NroDocumento', 'Lote', 'Cantidad', 'TipoAjuste'
}

AJUSTES_ENTRADA_HEADER = AJUSTES_SALIDA_HEADER.union({'Precio', 'FechaVencimiento'})

TRASLADOS_HEADER = {
    'FechaTraslado', 'CentroOrigen', 'CentroDestino', 'Cantidad',
    'Lote', 'Plu', 'NroDocumento'
}

DISPENSACIONES_ANULADAS_HEADER = {
    'NroSSC', 'FechaAnulacion', 'NIT', 'CECO', 'NroDocumento', 'Plan', 'SubPlan',
    'TipodeIdentidad', 'NroDocumentoAfiliado', 'Beneficiario', 'CategoriaActual',
    'NroAutorizacion', 'Mipres', 'Plu', 'Articulo', 'Cantidad', 'Lote', 'Precio',
    'UsuarioDispensa', 'TipodeConcepto', 'FechaVencimiento'
}

COMPRAS_HEADER = {
    'FechaCompra', 'NroDocumento', 'NIT', 'CECO', 'Factura', 'Plu', 'Lote', 'Precio',
    'FechaVencimiento'
}

AJUSTE_LOTE_HEADER = {
    'FechaVencimiento', 'Plu', 'Lote'
}

PAGOS_RECIBIDOS_HEADER = {
    'FechaPago', 'NIT', 'Valor'
}

AJUSTES_ENTRADA_PRUEBA_HEADER = {
    'despacho', 'fecha_tras', 'bodega_salida', 'bodega_ent',
    'codigo', 'cantidad', 'lote', 'fecha_venc', 'Costo',
    'usuario',
}

# SAP INFORMATION

SAP_USER = config('SAP_USER')
SAP_PASS = config('SAP_PASS')
SAP_COMPANY = config('SAP_COMPANY')
SAP_URL = config('SAP_URL')
