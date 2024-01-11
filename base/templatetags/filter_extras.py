import re

from django import template
from django.utils.safestring import mark_safe

register = template.Library()


def make_text_status(item: list) -> str:
    """
    Crea el texto correspondiente a los posibles status
    de aquel registro, sean errores o no con base en la lista
    proveniente del objeto Csv2Dict y su atributo data.
    :param item: Lista con tantos diccionários como subitens de aquel
                 registro.
            Ex.:
              [
                {'Beneficiario': '', 'CECO': '', 'Cantidad Dispensada': '',
                  'Categoria Actual': '', 'Factura': '', 'Fecha Factura': '',
                  'Lote': '', 'Mipres': '' 'NIT': '', 'No SSC': '', 'Nombre': '',
                  'Nro Documento': '', 'Numero Autorizacion': '',
                  'Observaciones': '' 'Plan': '', 'Plu': '',
                  'Status': '',
                  'SubPlan': '', 'Tipo de Identificacion': '', 'Vlr.Unitario Margen': ''
                  },
                {'Beneficiario': '', 'CECO': '', 'Cantidad Dispensada': '',
                  'Categoria Actual': '', 'Factura': '', 'Fecha Factura': '',
                  'Lote': '', 'Mipres': '' 'NIT': '', 'No SSC': '', 'Nombre': '',
                  'Nro Documento': '', 'Numero Autorizacion': '',
                  'Observaciones': '' 'Plan': '', 'Plu': '',
                  'Status': '[CSV] Plu no valido al ser consultado con SSC 3740846',
                  'SubPlan': '', 'Tipo de Identificacion': '', 'Vlr.Unitario Margen': ''
                  },
                 {'Beneficiario': '', 'CECO': '', 'Cantidad Dispensada': '',
                  'Categoria Actual': '', 'Factura': '', 'Fecha Factura': '',
                  'Lote': '', 'Mipres': '' 'NIT': '', 'No SSC': '', 'Nombre': '',
                  'Nro Documento': '', 'Numero Autorizacion': '',
                  'Observaciones': '' 'Plan': '', 'Plu': '',
                  'Status': '[CSV] No se encontraron entregas para SSC 3740846 | [CSV] SubPlan '
                            "no reconocido para contrato 'Evento PBS Subsdiado'",
                  'SubPlan': '', 'Tipo de Identificacion': '', 'Vlr.Unitario Margen': ''
                  }
              ]
    :return: Texto con toda la información de los Status.
    """
    messages = []
    for i in item:
        if status := i['Status']:
            phrases = status.split('|')
            for phrase in phrases:
                if phrase.strip() not in messages:
                    messages.append(phrase.strip())
    return mark_safe(', '.join(messages))


@register.filter
def len_lists_in_dict(item: dict) -> int:
    """A partir de um diccionario, que contiene por cada llave
    una lista, se suman los len de todos los values
    asumiendo que todos sus values son listas."""
    return sum(len(lst) for lst in item.values())


@register.filter
def format_txt(txt: str, arg) -> str:
    """
    Aplicar el 'format' própio de python.
    Ej:
        txt='Se registraron {} tipos de errores, los cuales no
        permitieron que se crearan los payloads necesarios para
        ser enviados a SAP.'
        arg='10'
    return: 'Se registraron 10 tipos de errores, los cuales
            no permitieron que se crearan los payloads
            necesarios para ser enviados a SAP.'
    """
    return txt.format(arg)


@register.filter
def clean(txt: str) -> str:
    """
    Aplica ciertos filtros para no mostrar cierta información en
    la plantilla
    """
    txt = txt.replace('[SAP]', '').strip()
    txt = txt.replace('[CSV]', '').strip()
    txt = txt.replace('[DocumentLines.ItemCode]', '').strip()
    txt = txt.replace('[DocumentLines.AccountCode]', '').strip()
    txt = re.sub(r"\[line:\s*\d+\]", '', txt)
    return txt


def sum_up_errs(txt: str) -> str:
    """
    Elimina cierto mensajes "importantes" de un determinado texto.
    Logrando resumir los errores cuando hay muchos de un mismo
    tipo.
    :param txt: Ej.:
                    "CECO no reconocido 112"
    :return: Ej:
                "CECO no reconocido"
    >>> sum_up_errs("CECO no reconocido 112")
    'CECO no reconocido.'
    >>> sum_up_errs("480000112 - El número de serie/lote 23089 seleccionado en la fila 1 no existe; especifique un número de serie/lote válido.")
    'Lote inválido.'
    >>> sum_up_errs("La cantidad recae en un inventario negativo  [DocumentLines.ItemCode][line: 3]")
    'Inventario negativo.'
     >>> sum_up_errs("No existen registros coincidentes (ODBC -2028) [4599096]")
     'No existen registros coincidentes.'
    >>> sum_up_errs("10001153 - Cantidad insuficiente para el artículo 300090055097 con el lote FY5874 en el almacén")
    'Cantidad insuficiente en artículo.'
    >>> sum_up_errs("No se encontraron entregas para SSC 4603881")
    'No se encontraron entregas en SSC.'
    """
    if 'CECO no reconocido' in txt:
        return 'CECO no reconocido.'

    elif 'No se encontraron entregas para SSC' in txt:
        return "No se encontraron entregas en SSC."

    elif 'especifique un número de serie' in txt:
        return "Lote inválido."

    elif 'inventario negativo' in txt:
        return "Cantidad recae en inventario negativo."

    elif 'registros coincidentes' in txt:
        return "No existen registros coincidentes."

    elif 'Cantidad insuficiente para el artículo' in txt:
        return "Cantidad insuficiente en artículo."

    elif 'No fue encontrado AbsEntry' in txt:
        return "No fue encontrado AbsEntry en lote."

    elif 'No fue encontrado BinEntry' in txt:
        return "No fue encontrado BinEntry."

    elif 'No se encontró dispensación para SSC' in txt:
        return "No se encontró dispensación para SSC."

    elif 'completamente asignado a la ubicación' in txt:
        'Artículo no está completamente asignado a ubicación.'

    return txt

@register.filter
def wrap_errors(list_errs: list[str]) -> set:
    """
    A partir de una lista con los errores,
    los intenta resumir para que no haya errores repetidos.
    :param list_errs: ['[CSV] CECO no reconocido 112', '[CSV] CECO no reconocido 920']
    :return: ['CECO no reconocido']
    """
    return {sum_up_errs(clean(err)) for err in list_errs}


@register.filter
def treat_invalid_lote(txt: str) -> str:
    """
    En los casos donde se recibe el siguiente texto en la plantilla:
    '480000112 - El número de serie/lote D070414 seleccionado en la fila
     1 no existe; especifique un número de serie/lote válido.'
     retorna el número de lote.
     >>> treat_invalid_lote("80000112 - El número de serie/lote D070414 seleccionado en la fila 1 no existe; especifique un número de serie/lote válido.")
     'D070414'
     >>> treat_invalid_lote("[SAP] 480000112 - El número de serie/lote 23089 seleccionado en la fila 1 no existe; especifique un número de serie/lote válido.")
     '23089'
    """
    if result := re.findall(r"lote\s(.*?)\sseleccionado", txt):
        return result[0]
    return txt

@register.filter
def extract_date(filename: str) -> str:
    """
    Dado el nombre de un archivo retorna la fecha
    contenida en el nombre del archivo.
    Ej.: Dispensacion202311172330.csv
    >>> extract_date('Dispensacion202311172330.csv')
    '2023-11-17'
    """
    if match := re.findall(r'\d+', filename):
        try:
            return f"{match[0][:4]}-{match[0][4:6]}-{match[0][6:8]}"
        except Exception:
            return ''
    return ''

