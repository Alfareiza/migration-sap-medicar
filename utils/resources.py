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


def mix_documentlines(data_sap: list, document_lines: list) -> list:
    """ Toma la información de SAP producto de haberse consultado y monta esa respuesta
    en un payload, rellenándolo con el resto de información necesaria. """
    for i in data_sap:
        # inicio procesos comunes #
        for key in ("id__", "U_LF_Formula", "CardCode", "U_LF_Autorizacion", "U_LF_IDSSC",
                    "LineStatus", "Dscription"):
            try:
                del i[key]
            except KeyError:
                ...
        i['BaseEntry'] = i['DocEntry']
        del i['DocEntry']
        i['BaseLine'] = i['LineNum']
        del i['LineNum']
        i['Price'] = [j['Price'] for j in document_lines if j['ItemCode'] == i['ItemCode']][0]
        i['CostingCode'] = document_lines[0]['CostingCode']
        i['CostingCode2'] = document_lines[0]['CostingCode2']
        i['CostingCode3'] = document_lines[0]['CostingCode3']
        i['WarehouseCode'] = document_lines[0]['WarehouseCode']
        i['BaseType'] = str(document_lines[0]['BaseType'])
        i['Quantity'] = int(i['Quantity'])
        # fin procesos comunes #

    return data_sap


def build_new_documentlines(data_sap: list, document_lines: list) -> list:
    """ Crea un nuevo document lines basado en la respuesta de sap.
    Obs.: Usado en notas_credito """
    resp = {}
    for s in data_sap:
        if s['LineNum'] not in resp:
            resp[s['LineNum']] = {
                "Price": [j['Price'] for j in document_lines if j['ItemCode'] == s['ItemCode']][0],
                "BaseLine": s['LineNum'],
                "ItemCode": s['ItemCode'],
                "BaseEntry": s['DocEntry'],
                "StockInmPrice": s['StockPrice'],
                "Quantity": s['Quantity'],
                "BaseType": document_lines[0]['BaseType'],
                "CostingCode": document_lines[0]['CostingCode'],
                "CostingCode2": document_lines[0]['CostingCode2'],
                "CostingCode3": document_lines[0]['CostingCode3'],
                "WarehouseCode": document_lines[0]['WarehouseCode'],
                "BatchNumbers": [
                    {
                        "Quantity": s['Quantity'],
                        "BatchNumber": s['BatchNum']
                    }
                ],
            }
        else:
            resp[s['LineNum']]['Quantity'] += s['Quantity']
            resp[s['LineNum']]['BatchNumbers'].append({'Quantity': s['Quantity'], 'BatchNumber': s['BatchNum']})
    return list(resp.values())


def re_make_stock_transfer_lines_traslados(stock_transfer_lines: list) -> list:
    """ A partir del document lines existente, crea uno nuevo """
    result = []
    for line, stock in enumerate(stock_transfer_lines[0]['BatchNumbers']):
        result.append({
            "LineNum": line,
            "ItemCode": stock_transfer_lines[0]['ItemCode'],
            "Quantity": stock['Quantity'],
            "BatchNumbers": [
                {
                    "BatchNumber": stock['BatchNumber'],
                    "Quantity": stock['Quantity']
                },
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "BinAbsEntry": stock_transfer_lines[0]['StockTransferLinesBinAllocations'][0]['BinAbsEntry'],
                    "Quantity": stock['Quantity'],
                    "BaseLineNumber": line,
                    "BinActionType": stock_transfer_lines[0]['StockTransferLinesBinAllocations'][0]['BinActionType'],
                    "SerialAndBatchNumbersBaseLine": stock_transfer_lines[0]['StockTransferLinesBinAllocations'][0][
                        'SerialAndBatchNumbersBaseLine'],
                },
                {
                    "BinAbsEntry": stock_transfer_lines[0]['StockTransferLinesBinAllocations'][1]['BinAbsEntry'],
                    "Quantity": stock['Quantity'],
                    "BaseLineNumber": line,
                    "BinActionType": stock_transfer_lines[0]['StockTransferLinesBinAllocations'][1]['BinActionType'],
                    "SerialAndBatchNumbersBaseLine": stock_transfer_lines[0]['StockTransferLinesBinAllocations'][1][
                        'SerialAndBatchNumbersBaseLine'],
                }
            ]
        })
    return result


if __name__ == '__main__':
    dl = [
        {
            "LineNum": 1,
            "ItemCode": "7702418000681",
            "Quantity": 100,
            "BatchNumbers": [
                {"BatchNumber": "C231343", "Quantity": 60},
                {"BatchNumber": "C232481", "Quantity": 40}
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "BinAbsEntry": 243,
                    "Quantity": 100,
                    "BaseLineNumber": 1,
                    "BinActionType": "batFromWarehouse",
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "BinAbsEntry": 197,
                    "Quantity": 100,
                    "BaseLineNumber": 1,
                    "BinActionType": "batToWarehouse",
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        }
    ]
    from pprint import pprint

    pprint(re_make_stock_transfer_lines_traslados(dl))
