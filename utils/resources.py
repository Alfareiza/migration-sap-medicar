import pickle
from datetime import datetime
from typing import List

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


def string_to_datetime(date_string):
    """Transform to datetime a string which depicts a datetime.
    >>> string_to_datetime('2022-12-31 18:36:00')
    datetime.datetime(2022, 12, 31, 18, 36)
    """
    return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")


def is_later_than_january_31_2024(date_obj) -> bool:
    """
    >>> is_later_than_january_31_2024(datetime(2022, 12, 31, 18, 36))
    False
    >>> is_later_than_january_31_2024(datetime(2024, 1, 31, 18, 36))
    False
    >>> is_later_than_january_31_2024(datetime(2024, 2, 1, 18, 36))
    True
    """
    january_31 = datetime(year=2024, month=1, day=31, hour=23, minute=59, second=59)
    return date_obj > january_31


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


def get_fibonacci_sequence(n: int, starting_number: int = 0) -> List[int]:
    """
    Method used to generate a Fibonacci sequence
    >>> get_fibonacci_sequence(5, 2)
    [2, 3, 5, 8, 13]
    >>> get_fibonacci_sequence(5)
    [0, 1, 1, 2, 3]
    >>> get_fibonacci_sequence(3, 5)
    [5, 6, 11]
    """
    fibonacci_sequence = [starting_number, starting_number + 1]
    for _ in range(2, n):
        next_num = fibonacci_sequence[-1] + fibonacci_sequence[-2]
        fibonacci_sequence.append(next_num)
    return fibonacci_sequence


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
    """A partir del StockTransferLines existente, crea uno nuevo donde no exista BatchNumbers con más de 1 lote.

    De esta manera el LineNum debe ser consistente, comenzando desde cero y terminando en cuantos lotes detectados
    en total en el StockTransferLinesBinAllocations."""
    result = []
    line = 0
    for stock in stock_transfer_lines:
        for batch in stock['BatchNumbers']:
            result.append({
                "LineNum": line,
                "ItemCode": stock['ItemCode'],
                "Quantity": batch['Quantity'],
                "BatchNumbers": [
                    {
                        "BatchNumber": batch['BatchNumber'],
                        "Quantity": batch['Quantity']
                    },
                ],
                "StockTransferLinesBinAllocations": [
                    {
                        "BinAbsEntry": stock['StockTransferLinesBinAllocations'][0]['BinAbsEntry'],
                        "Quantity": batch['Quantity'],
                        "BaseLineNumber": line,
                        "BinActionType": stock['StockTransferLinesBinAllocations'][0]['BinActionType'],
                        "SerialAndBatchNumbersBaseLine": stock['StockTransferLinesBinAllocations'][0][
                            'SerialAndBatchNumbersBaseLine'],
                    },
                    {
                        "BinAbsEntry": stock['StockTransferLinesBinAllocations'][1]['BinAbsEntry'],
                        "Quantity": batch['Quantity'],
                        "BaseLineNumber": line,
                        "BinActionType": stock['StockTransferLinesBinAllocations'][1]['BinActionType'],
                        "SerialAndBatchNumbersBaseLine": stock['StockTransferLinesBinAllocations'][1][
                            'SerialAndBatchNumbersBaseLine'],
                    }
                ]
            })
            line += 1
    return result


if __name__ == '__main__':
    from pprint import pprint

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
    dl_test = [
        {
            "LineNum": 0,
            "ItemCode": "5420072711420",
            "Quantity": 5,
            "BatchNumbers": [
                {
                    "Quantity": 5,
                    "BatchNumber": "QSABPAD0"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 5,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 0,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 5,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 0,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 1,
            "ItemCode": "6921875005966",
            "Quantity": 4,
            "BatchNumbers": [
                {
                    "Quantity": 4,
                    "BatchNumber": "243132082"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 4,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 1,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 4,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 1,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 2,
            "ItemCode": "7594002621989",
            "Quantity": 8,
            "BatchNumbers": [
                {
                    "Quantity": 8,
                    "BatchNumber": "8196"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 8,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 2,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 8,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 2,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 3,
            "ItemCode": "7702057710620",
            "Quantity": 90,
            "BatchNumbers": [
                {
                    "Quantity": 90,
                    "BatchNumber": "3D1435"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 90,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 3,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 90,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 3,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 4,
            "ItemCode": "7702057715342",
            "Quantity": 1,
            "BatchNumbers": [
                {
                    "Quantity": 1,
                    "BatchNumber": "4E1636"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 1,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 4,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 1,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 4,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 5,
            "ItemCode": "7702184011966",
            "Quantity": 210,
            "BatchNumbers": [
                {
                    "Quantity": 210,
                    "BatchNumber": "E030412"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 210,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 5,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 210,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 5,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 6,
            "ItemCode": "7702184030141",
            "Quantity": 2,
            "BatchNumbers": [
                {
                    "Quantity": 2,
                    "BatchNumber": "E070077"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 2,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 6,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 2,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 6,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 7,
            "ItemCode": "7702184459751",
            "Quantity": 280,
            "BatchNumbers": [
                {
                    "Quantity": 280,
                    "BatchNumber": "E060266"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 280,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 7,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 280,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 7,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 8,
            "ItemCode": "7702184693353",
            "Quantity": 430,
            "BatchNumbers": [
                {
                    "Quantity": 430,
                    "BatchNumber": "E050824"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 430,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 8,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 430,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 8,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 9,
            "ItemCode": "7703038065944",
            "Quantity": 124,
            "BatchNumbers": [
                {
                    "Quantity": 124,
                    "BatchNumber": "89802"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 124,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 9,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 124,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 9,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 10,
            "ItemCode": "7703038065982",
            "Quantity": 120,
            "BatchNumbers": [
                {
                    "Quantity": 120,
                    "BatchNumber": "92828"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 120,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 10,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 120,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 10,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 11,
            "ItemCode": "7703153000622",
            "Quantity": 20,
            "BatchNumbers": [
                {
                    "Quantity": 20,
                    "BatchNumber": "1507770"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 20,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 11,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 20,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 11,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 12,
            "ItemCode": "7703153032890",
            "Quantity": 10,
            "BatchNumbers": [
                {
                    "Quantity": 10,
                    "BatchNumber": "1489416"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 10,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 12,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 10,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 12,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 13,
            "ItemCode": "7703763999064",
            "Quantity": 47,
            "BatchNumbers": [
                {
                    "Quantity": 47,
                    "BatchNumber": "3311704"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 47,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 13,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 47,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 13,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 14,
            "ItemCode": "7706127005197",
            "Quantity": 10,
            "BatchNumbers": [
                {
                    "Quantity": 10,
                    "BatchNumber": "905864"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 10,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 14,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 10,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 14,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 15,
            "ItemCode": "7707019396195",
            "Quantity": 6,
            "BatchNumbers": [
                {
                    "Quantity": 6,
                    "BatchNumber": "108123"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 6,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 15,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 6,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 15,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 17,
            "ItemCode": "7707172689462",
            "Quantity": 6,
            "BatchNumbers": [
                {
                    "Quantity": 3,
                    "BatchNumber": "HE2L"
                },
                {
                    "Quantity": 3,
                    "BatchNumber": "YC6V"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 6,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 17,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 6,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 17,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 18,
            "ItemCode": "7707177973115",
            "Quantity": 85,
            "BatchNumbers": [
                {
                    "Quantity": 85,
                    "BatchNumber": "113A27"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 85,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 18,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 85,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 18,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 19,
            "ItemCode": "7707193642309",
            "Quantity": 16,
            "BatchNumbers": [
                {
                    "Quantity": 16,
                    "BatchNumber": "0122"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 16,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 19,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 16,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 19,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        },
        {
            "LineNum": 20,
            "ItemCode": "7707288823675",
            "Quantity": 40,
            "BatchNumbers": [
                {
                    "Quantity": 40,
                    "BatchNumber": "4A603"
                }
            ],
            "StockTransferLinesBinAllocations": [
                {
                    "Quantity": 40,
                    "BinAbsEntry": 196,
                    "BinActionType": "batFromWarehouse",
                    "BaseLineNumber": 20,
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {
                    "Quantity": 40,
                    "BinAbsEntry": 382,
                    "BinActionType": "batToWarehouse",
                    "BaseLineNumber": 20,
                    "SerialAndBatchNumbersBaseLine": 0
                }
            ]
        }
    ]

    pprint(re_make_stock_transfer_lines_traslados(dl_test))
