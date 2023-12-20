import datetime
import json
import pickle
from typing import List

import requests
from requests import HTTPError, Timeout

from core.settings import BASE_DIR, SAP_COMPANY, SAP_USER, SAP_PASS, SAP_URL
from core.settings import logger as log
from utils.decorators import login_required
from utils.resources import clean_text, moment

login_pkl = BASE_DIR / 'login.pickle'


class SAP:
    def __init__(self, module):
        self.module = module  # Instancia de clase Module
        self.sess_id = ''
        self.sess_timeout = None

    # @logtime('API')
    def request_api(self, method, url, headers, payload={}) -> dict:
        # sourcery skip: raise-specific-error
        try:
            res = {"ERROR": ""}
            response = requests.request(method, url, headers=headers,
                                        data=json.dumps(payload),
                                        timeout=120)
            response.raise_for_status()
        except Timeout:
            txt = "No hubo respuesta de la API 2 en min."
            # log.error(txt)
            res = {"ERROR": txt}
        except HTTPError as e:
            if 'application/json' in e.response.headers['Content-Type']:
                err = e.response.json()
            else:
                err = e.response.content
            # extra_txt = payload['U_LF_Formula'] if 'U_LF_Formula' in payload else ''
            # tag = f'[{self.module.name}] ' if self.module else ''
            # log.error(f"{tag}{err['error']['message']} [{extra_txt}]"
            if 'error' in err and err.get('error'):
                msg = err['error']['message']
            else:
                msg = str(err)
            res = {"ERROR": clean_text(msg)}
        except Exception as e:
            txt = f"{str(e)}"
            # log.error(txt)
            res = {"ERROR": txt}
        else:
            if response.text:
                res = response.json()
            else:
                res = {'DocEntry': 'Sin DocEntry'}
        finally:
            return res

    def login(self) -> bool:
        """
        Realiza el login ante la API de SAP, asignándole el atributo
        self.sess_id y self.sess_timeout a partir de la respuesta de la
        API de SAP.
        Es llamado en su mayoría desde el decorator @login_required
        La respuesta exitosa luce así:
            {
                '@odata.context': 'https://123456.heinsohncloud.com.
                co:50000/b1s/v2/$metadata#B1Sessions/$entity',
                'SessionId': '3643564asd-0181-11ee-8000-6045b3645e5bd5',
                'SessionTimeout': 30,
                'Version': '1000191'
            }
        :return: True or False
        """
        payload = {
            "CompanyDB": SAP_COMPANY,
            "UserName": SAP_USER,
            "Password": SAP_PASS,
            "Language": 25
        }
        headers = {'Content-Type': 'application/json'}
        log.info("...Realizando login")
        resp = self.request_api(
            'POST',
            f"{SAP_URL}/Login",
            headers=headers,
            payload=payload,
        )
        if resp.get('SessionId'):
            self.sess_id = resp['SessionId']
            self.sess_timeout = moment() + datetime.timedelta(minutes=resp['SessionTimeout'] - 1)
            log.info(f"Login realizado {format(moment(), '%r')}, se vencerá a las {format(self.sess_timeout, '%r')}")
            with open(login_pkl, 'wb') as f:
                pickle.dump([self.sess_id, self.sess_timeout], f)
            return True
        else:
            return False

    def set_header(self):
        return {
            'Content-Type': 'application/json',
            'Cookie': f"B1SESSION={self.sess_id}"
        }

    def post(self, item: dict, url: str) -> dict:
        """Realiza el post ante la API de SAP y retorna
        lo que haya resultado de la función request_api"""
        headers = self.set_header()
        return self.request_api('POST', url,
                                headers=headers, payload=item)

    def get(self, url):
        headers = self.set_header()
        return self.request_api('GET', url, headers=headers)

    def patch(self, item: dict, url: str) -> dict:
        headers = self.set_header()
        return self.request_api('PATCH', url,
                                headers=headers, payload=item)


class SAPData(SAP):
    BASE_URL = SAP_URL
    SUCURSAL = '/sml.svc/InfoSucursalV2Query'
    ABSENTRY = '/sml.svc/InfoUbicacionV2Query'
    EMBALAJE = '/sml.svc/InfoEmbalajeV2Query'
    LOTE = '/sml.svc/InfoLoteV2Query'
    FACTURA = '/sml.svc/InfoFacturaV2Query'
    DISPENSADO = '/sml.svc/InfoDispensadoV2Query'

    def __init__(self, module=None):
        super().__init__(module)
        self.sucursales = {}
        self.sucursales_loaded = False
        self.entregas = {}
        self.entregas_loaded = False
        self.abs_entries = {}
        self.abs_entries_loaded = False
        self.dispensados = {}
        self.dispensados_loaded = False

    # Se podria agregar un post_init que de manera asíncrona
    # ejecute load_sucursales y load_abs_entries

    @login_required
    def get_all(self, end_url: object) -> List:
        """
        Carga todos los registros de BASE_URL + end_url
        :param end_url: Final de url que será llamada.
                    Ej.: '/sml.svc/SucursalQuery'
        :return: Lista con todos los registros capturados.
        """
        all_records = []
        flag = True
        res = self.get(self.BASE_URL + end_url)
        while flag:
            if not res.get('ERROR'):
                if res.get('value'):
                    all_records.extend(res['value'])
                if '@odata.nextLink' in res and res.get('@odata.nextLink'):
                    _, to_skip = res['@odata.nextLink'].rsplit('skip=')
                    res = self.get(self.BASE_URL + end_url + f'?$skip={to_skip}')
                else:
                    break
            else:
                flag = False
        return all_records

    @login_required
    def load_sucursales(self):
        """
        Carga en el atributo self.sucursales todas las sucursales en SAP.
        con el siguiente formato:
        {
            '100': {
                "WhsCode": "100",
                "State": "23",
                "U_HBT_Dimension1": "COR",
                "U_HBT_Dimension2": "100",
                "id__": 1
            },
            {...},
            {...},
        }
        :return:
        """
        log.info('Cargando todas las sucursales.')
        if sucursales := self.get_all(self.SUCURSAL):
            for sucursal in sucursales:
                if sucursal['WhsCode'] not in self.sucursales:
                    self.sucursales[sucursal['WhsCode']] = {}
                self.sucursales[sucursal['WhsCode']].update(**sucursal)
        else:
            log.warning(f'No se encontraron sucursales en {self.SUCURSAL}.')
        # log.info('Proceso de cargar sucursales finalizado.')
        self.sucursales_loaded = True

    @login_required
    def load_abs_entries(self):
        """
        Carga en el atributo self.sucursales todas las AbsEntry de SAP.
        De esta manera a partir de una lista de diccionarios
        como la siguiente:
        [
            {
                'AbsEntry': 91,
                'BinCode': '100-SYSTEM-BIN-LOCATION',
                'WhsCode': '100',
                'id__': 1
            }
            {...}
        ]
        Le asigna el valor a self.sucursales con lo siguiente:
        {
            '100': {'100-AL': 451, '100-SYSTEM-BIN-LOCATION': 91, '100-TR': 361},
            '101': {'101-AL': 452, '101-SYSTEM-BIN-LOCATION': 92, '101-TR': 362},
            '102': {'102-AL': 453, '102-SYSTEM-BIN-LOCATION': 93, '102-TR': 363}
            ...
        }
        Siendo cada llave del diccionario un centro.
        """
        log.info('Cargando todas las AbsEntry y BinCode de las bodegas.')
        if bodegas := self.get_all(self.ABSENTRY):
            for bod in bodegas:
                # bod = {'AbsEntry': 91, 'BinCode': '100-SYSTEM-BIN-LOCATION', 'WhsCode': '100', 'id__': 1}
                if bod['WhsCode'] not in self.abs_entries:
                    self.abs_entries[bod['WhsCode']] = {}
                self.abs_entries[bod['WhsCode']][bod['BinCode']] = bod['AbsEntry']
        else:
            log.warning(f'No se encontraron ABSENTRIES en {self.ABSENTRY}.')
        # log.info('Proceso de cargar sucursales finalizado.')
        self.abs_entries_loaded = True

    @login_required
    def load_dispensados(self):
        """
        Carga en la variable self.dispensados, las dispensaciones
        realizadas según información obtenida en SAP.
        Usado para módulo facturacion
        De esta manera a partir de una lista de diccionarios
        como la siguiente:
        [
            {
                'CardCode': 'CL1045',
                'DocEntry': 10,
                'U_LF_Autorizacion': '5F5SDFDDDD5F',
                'U_LF_Formula': '4D65D55536d',
                'U_LF_IDSSC': None,
                'id__': 1
            }
            {...}
        ]
        Le asigna el valor a self.dispensados con lo siguiente:
        {
            '4D65D55536d': {'CardCode': 'CL1045', 'DocEntry': 10, 'U_LF_Autorizacion': '5F5SDFDDDD5F',
                            'U_LF_Formula': '4D65D55536d', 'U_LF_IDSSC': None, 'id__': 1},
            ...
        }
        Siendo cada llave del diccionario es un ssc o conocido dentro de SAP como U_LF_Formula.
        """
        log.info('Cargando todos los dispensados.')
        if dispensados := self.get_all(self.DISPENSADO):
            for dispensado in dispensados:
                if dispensado['U_LF_Formula'] not in self.dispensados:
                    self.dispensados[dispensado['U_LF_Formula']] = {}
                self.dispensados[dispensado['U_LF_Formula']] = dispensado
        else:
            log.warning(f'No se encontraron dispensaciones en {self.DISPENSADO}.')
        # log.info('Proceso de cargar sucursales finalizado.')
        self.dispensados_loaded = True

    @login_required
    def load_info_ssc(self, ssc: str):
        """
        Carga en la variable self.entregas, las entregas
        correspondientes a un ssc según información obtenida
        en SAP.
        """
        # log.info(f'Cargando todas las entregas de {ssc}.')
        res = self.get(self.BASE_URL + f"{self.FACTURA}?$filter=U_LF_Formula eq '{ssc}'")
        if not res.get('ERROR'):
            if entregas := res.get('value'):
                self.entregas[ssc] = entregas
            else:
                log.warning(f'No se encontraron entregas en {ssc}.')
        # log.info('Proceso de cargar entregas finalizado.')
        self.entregas_loaded = True

    def get_costing_code_from_sucursal(self, ceco: str) -> str:
        """
        Retorna el costing code (U_HBT_Dimension1) a partir del
        CECO (WhsCode).
        :param ceco: "304"
        :return: "BOL", "RIO", o "" en caso de no encontrar el CECO.
        """
        if sucursal := self.sucursales.get(ceco):
            return sucursal['U_HBT_Dimension1']
        elif not self.sucursales and not self.sucursales_loaded:
            self.load_sucursales()
            return self.get_costing_code_from_sucursal(ceco)
        else:
            return ''

    def get_info_ssc(self, value: str) -> list:
        """
        Busca en la vista de SAP las entregas que tuvo un SSC
        :param value: SSC.
        :return: Lista con entregas de SSC.
        """
        if value in self.entregas:
            return self.entregas.get(value)
        elif not self.entregas and not self.entregas_loaded:
            self.load_info_ssc(value)
            return self.get_info_ssc(value)
        else:
            return []

    def get_docentry_factura(self, ssc: str) -> str:
        """
        A partir de un número de ssc, consigue el DocEntry,
        caso contrario retorna un ''
        """
        if res := self.dispensados.get(ssc):
            return str(res['DocEntry'])
        elif not self.dispensados and not self.dispensados_loaded:
            self.load_dispensados()
            return self.get_docentry_factura(ssc)
        else:
            return ''

    def get_bin_abs_entry_from_ceco(self, ceco: str) -> int:
        """
        Usado desde el modulo de traslados busca el AbsEntry
        de determinada ubicación.
        :param ceco: Suele ser bodega destino o bodega origen.
                Ex: '304, '101', etc.
        :return: Caso encontrar el AbsEntry del value, retorna
                 el valor encontrado en self.sucursales, sino un cero.
        """
        if ceco in self.abs_entries and self.abs_entries[ceco].get(f"{ceco}-AL"):
            return self.abs_entries[ceco].get(f"{ceco}-AL")
        elif not self.abs_entries and not self.abs_entries_loaded:
            self.load_abs_entries()
            return self.get_bin_abs_entry_from_ceco(ceco)
        else:
            return 0

    @login_required
    def get_embalaje_info_from_plu(self, plu: str) -> list:
        """
        Usado desde Compras, consulta la API de SAP para obtener
        informacioón de embalaje de determinado Plu.
        :param plu: Identificación de um articulo.
        :return: Lista de dicts donde cada dicts puede ser así:
                 [
                    {
                        "ItemCode": "7703763279029",
                        "PurPackMsr": "CAJ",
                        "PurPackUn": 1.0,
                        "id__": 1
                    }
                ]
        """
        if embalaje_info := self.get_all(f"{self.EMBALAJE}?$filter=ItemCode eq '{plu}'"):
            return embalaje_info
        log.warning(f'No se encontró info de embalaje para el plu {plu!r}.')
        return []

    @login_required
    def get_bin_abs_entry_from_lote(self, lote: str) -> int:
        """
        Usado desde el modulo de ajuste lote busca el AbsEntry
        de determinada lote, llamando la vista InfoLoteQuery y
        filtrando por el lote ?$filter=DistNumber eq 'A346669G'.
        Al llamar la API, retorna un json así:
            {
                "AbsEntry": 143,
                "ItemCode": "17708926054472",
                "DistNumber": "A346669G",
                "id__": 1
            }
        Del cual será tomado el AbsEntry.
        :param lote: .
                Ex: 'A346669G, '106', 'ME3029', etc.
        :return: Caso encontrar el AbsEntry del value, retorna
                 el valor encontrado.
        """
        if lote_info := self.get_all(f"{self.LOTE}?$filter=DistNumber eq '{lote}'"):
            return lote_info[0]['AbsEntry']
        log.warning(f'No se encontró info del lote {lote!r} en SAP.')
        return 0


if __name__ == '__main__':
    client = SAPData()
    # client.get_costing_code_from_sucursal('1001')
    # client.load_abs_entries()
    # client.load_sucursales()
    # client.load_dispensados()
    client.get_docentry_factura('4694768')
