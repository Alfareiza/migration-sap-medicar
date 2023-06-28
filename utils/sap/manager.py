import datetime
import json
import pickle

import requests
from requests import HTTPError, Timeout

from core.settings import BASE_DIR, logger as log
from utils.resources import clean_text, datetime_str, moment

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
            response = requests.request(method, url, headers=headers,
                                        data=json.dumps(payload),
                                        timeout=20)
            response.raise_for_status()
        except Timeout:
            log.error(txt := "No hubo respuesta de la API en 20 segundos.")
            res = {"ERROR": txt}
        except HTTPError as e:
            err = e.response.json()
            extra_txt = payload['U_LF_Formula'] if 'U_LF_Formula' in payload else ''
            log.error(f"[{self.module.name}], {err['error']['message']} [{extra_txt}]")
            res = {"ERROR": clean_text(err['error']['message'])}
        except Exception as e:
            log.error(txt := f"{str(e)}")
            res = {"ERROR": txt}
        else:
            res = response.json()
        finally:
            return res

    def login(self) -> bool:
        """
        Realiza el login ante la API de SAP, asignándole el atributo
        self.sess_id y self.sess_timeout a partir de la respuesta de la
        API de SAP.
        La respuesta exitosa luce así:
            {
                '@odata.context': 'https://vm-hbt-hm34.heinsohncloud.com.
                co:50000/b1s/v2/$metadata#B1Sessions/$entity',
                'SessionId': 'ad7afdee-0181-11ee-8000-6045bd7e5bd5',
                'SessionTimeout': 30,
                'Version': '1000191'
            }
        :return: True or False
        """
        payload = {
            "CompanyDB": "PRUEBAS_LOGIFARMA_FEB2",
            "UserName": "medicar",
            "Password": "1234",
            "Language": 25
        }
        headers = {'Content-Type': 'application/json'}
        log.info("...Realizando login")
        resp = self.request_api(
            'POST',
            "https://vm-hbt-hm34.heinsohncloud.com.co:50000/b1s/v2/Login",
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

    def login_if_necessary(self):
        """
        Verifica que archivo de login exista, en caso de que no
        hace login y lo crea.
        :return:
        """
        log.info('Verificando si se debe hacer login o no ...')
        if not login_pkl.exists():
            return self.login()
        else:
            with open(login_pkl, 'rb') as f:
                sess_id, sess_timeout = pickle.load(f)
                now = moment()
                if now > sess_timeout:
                    log.info('Login vencido ...')
                    return self.login()
                else:
                    log.info(f"Login válido. {datetime_str(now)} es menor que {datetime_str(sess_timeout)}")
                    self.sess_id = sess_id
                    return True

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

class SAPData(SAP):

    BASE_URL = 'https://vm-hbt-hm34.heinsohncloud.com.co:50000/b1s/v2'
    SUCURSAL = '/sml.svc/SucursalQuery'

    def __init__(self, module=None):
        super().__init__(module)
        self.sucursales = {}
        self.entregas = {}

    def get_all(self, end_url) -> list:
        """
        Carga recursivamente todos los registros de BASE_URL + end_url
        :param end_url: Final de url que será llamada.
                    Ej.: '/sml.svc/SucursalQuery'
        :return: Lista con todos los registros capturados.
        """
        all = []
        flag = True
        res = self.get(self.BASE_URL + end_url)
        while flag:
            if not res.get('ERROR'):
                if res.get('value'):
                    all.extend(res['value'])
                if '@odata.nextLink' in res and res.get('@odata.nextLink'):
                    _, to_skip = res['@odata.nextLink'].rsplit('skip=')
                    res = self.get(self.BASE_URL + self.SUCURSAL + f'?$skip={to_skip}')
                else:
                    break
            else:
                flag = False
        return all

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
        if self.login_if_necessary():
            log.info('Cargando todas las sucursales.')
            sucursales = self.get_all(self.SUCURSAL)
            for s in sucursales:
                self.sucursales[s['WhsCode']] = s
            else:
                log.info('No se encontraron sucursales.')
            # log.info('Proceso de cargar sucursales finalizado.')
        else:
            log.error("No fue posible hacer login para cargar las sucursales.")


    def get_costing_code_from_surcusal(self, ceco: str) -> str:
        """
        Retorna el costing code (U_HBT_Dimension1) a partir del
        CECO (WhsCode).
        :param ceco: "304"
        :return: "BOL", "RIO", o "" en caso de no encontrar el CECO.

        """
        if sucursal := self.sucursales.get(ceco):
            return sucursal['U_HBT_Dimension1']
        elif not self.sucursales:
            self.load_sucursales()
            if sucursal := self.sucursales.get(ceco):
                return sucursal['U_HBT_Dimension1']
            else:
                return ''
        else:
            return ''

    def load_docentries_ssc(self, ssc):
        if self.login_if_necessary():
            log.info(f'Cargando todas las entregas de {ssc}.')
            qry = f"/sml.svc/InfoEntregaQuery?$filter=U_LF_Formula eq '{ssc}'"
            res = self.get(self.BASE_URL + qry)
            if not res.get('ERROR'):
                if entregas := res.get('value'):
                    self.entregas[ssc] = entregas
                else:
                    log.info(f'No se encontraron entregas en {ssc}.')
            # log.info('Proceso de cargar entregas finalizado.')
        else:
            log.error("No fue posible hacer login para cargar las sucursales.")
    def get_docentry_entrega(self, value) -> list:
        """
        Busca en la vista de SAP las entregas que tuvo un SSC
        :param value: SSC.
        :return: Lista con entregas de SSC.
        """
        if value in self.entregas:
            return self.entregas.get(value)
        elif not self.entregas:
            self.load_docentries_ssc(value)
            if value in self.entregas:
                return self.entregas.get(value)
            else:
                return []
        else:
            self.load_docentries_ssc(value)
            if value in self.entregas:
                return self.entregas.get(value)
            else:
                return []



if __name__ == '__main__':
    client = SAPData()
    client.get_costing_code_from_surcusal('1001')
