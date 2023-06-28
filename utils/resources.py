from datetime import datetime
from pytz import timezone


def moment():
    return datetime.now(tz=timezone('America/Bogota'))


def set_filename_now(filename, extra_text='') -> str:
    """
    Dado un nombre de archivo le agrega una 'razón' al nombre del mismo.
    :param filename: Ex.: 'myfile.csv'
    :param extra_text: Ex.: 'procesados'
    :return: 'myfile_procesados.csv'
    """
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
