import pickle
from datetime import datetime

from django.conf import settings
from pytz import timezone
from decouple import config

from core.settings import BASE_DIR, logger


def moment():
    return datetime.now(tz=timezone('America/Bogota'))


def set_filename_now(filename, extra_text='') -> str:
    """
    Dado un nombre de archivo le agrega una 'razón' al nombre del mismo.
    :param filename: Ex.: 'myfile.csv'
    :param extra_text: Ex.: 'procesados'
    :return: 'myfile_procesados.csv'
    """
    # En caso el archivo tenga varios puntos a lo largo del nombre
    # el programa solo le remplazará el primero por un espacio vacío
    if filename.count('.') > 1:
        filename = filename.replace('.', '', 1)
    name, ext = filename.rsplit('.')
    if extra_text:
        name += f"_{extra_text}"
        # {format(moment, '_%H_%M_%S')}
    return f"{name}.{ext}"


def datetime_str(dt=None):
    """ Transforma la fecha en un formato específico. """
    if not dt:
        dt = moment()
    return format(dt, '%G-%m-%d %H:%M:%S')


def set_filename(name: str, reason: str) -> str:
    """
    Dado un nombre de archivo le agrega una 'razón'
    y un registro de hora, dia, segundos
    :param name: Ex.: 'myfile.csv'
    :param reason: 'errores'
    :return: 'myfile_errores.csv'
    """
    return set_filename_now(name,
                            extra_text=f"{reason}_{format(moment(), '_%H_%M_%S')}"
                            )


def clean_text(text) -> str:
    """
    Remove accents and colons from the frase
    :param text: "vÃ¡lida"
    :return: "valida"
    """
    return text.replace(',', '.')


def load_comments(row, column_name=None) -> str:
    txt = f"Cargue automático {datetime_str()} UsuarioSAP: {config('SAP_USER')}"
    if column_name:
        extra = row.get(column_name)
        txt += f" ({column_name}. {extra})"
    return txt


def beautify_name(name):
    new_name = name.split('_')
    return ' '.join(new_name).title()


def format_number(num: int) -> str:
    """
    Format a number with points
    >>> format_number(10)
    '10'
    >>> format_number(1000)
    '1.000'
    >>> format_number(1000000)
    '1.000.000'
    """
    return f"{num:,.0f}".replace(',', '.')


def has_ceco(name, item, ceco='391'):
    modules_to_check = settings.MODULES_USE_DOCUMENTLINES.copy()
    modules_to_check.remove(settings.FACTURACION_NAME)
    if name in modules_to_check:
        for line in item['DocumentLines']:
            if 'WarehouseCode' in line.keys() and line['WarehouseCode'] == ceco:
                return True
    elif name == settings.TRASLADOS_NAME:
        if item['FromWarehouse'] == ceco or item['ToWarehouse'] == ceco:
            return True
    return False


def login_check(sap) -> bool:
    """
    1. Valida que exista el archivo de login:
        1.1 Caso exista:
                1.1.1 Valida que que la hora de sesión
                        no sea mayor que la hora actual.
                1.1.2 Caso sea mayor, efectua el login.
        1.2 Caso no exista, efectua el login.
    Puede retornar False cuando la API que logra el login este
    caída.
    :param sap: Instancia de SAPData
    :return: True o False caso haga login o no.
    """
    login_pkl = BASE_DIR / 'login.pickle'

    if not login_pkl.exists():
        logger.info('Cache de login no encontrado')
        login_succeed = sap.login()
    else:
        with open(login_pkl, 'rb') as f:
            sess_id, sess_timeout = pickle.load(f)
            now = moment()
            if now > sess_timeout:
                logger.warning('Tiempo de login anterior expiró')
                login_succeed = sap.login()
            else:
                # log.info(f"Login válido. {datetime_str(now)} es menor que {datetime_str(sess_timeout)}")
                sap.sess_id = sess_id
                login_succeed = True
    return login_succeed
