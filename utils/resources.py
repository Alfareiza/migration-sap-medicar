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
    txt = f"Cargue automático {datetime_str()} UsuarioSAP: Medicar"
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
