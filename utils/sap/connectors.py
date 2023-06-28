from concurrent.futures import ThreadPoolExecutor

from core.settings import logger as log
from utils.decorators import logtime
from utils.sap.manager import SAP


class SAPConnect(SAP):
    def __init__(self, module):
        super().__init__(module)
        self.info = None  # Instancia de clase Csv2Dict

    def process(self, csv_to_dict):
        if self.login_if_necessary():
            self.info = csv_to_dict
            self.register()
            # self.register_sync()
            log.info(f"Procesados {len(self.info.succss)} peticiones a API de SAP.")
        else:
            log.info('No fue posible hacer login en SAP')

    @logtime('MASSIVE POSTS')
    def register(self):
        with ThreadPoolExecutor(max_workers=12) as executor:
            _ = [executor.submit(
                    self.post_info,
                    key, self.info.data[key]['json'],
                    self.module.url[find_key_by_value(self.module.series, self.info.data[key]['json']['Series'])]
                )
                  for key in list(self.info.succss)]

    def register_sync(self):
        for i, key in enumerate(list(self.info.succss), 1):
            log.info(f'Posting {i} {key}')
            self.post_info(key, self.info.data[key]['json'],
                           self.module.url[
                               find_key_by_value(self.module.series, self.info.data[key]['json']['Series'])
                           ])

    def post_info(self, key, item, url):
        """
        Realiza un post guarda el registro de si fue exitoso o no.
        :param key: '1127507'
        :param item: {'Series': 81, 'U_HBT_Tercero': 'CL901543211', 'Comments': 'Escenario Dispensación Medicar', 'U_LF_Plan': 'S', 'U_LF_IdAfiliado': '1081818326', 'U_LF_NombreAfiliado': 'LAUREN SOFIA PATERNINA ARROYO', 'U_LF_Formula': '1127507', 'U_LF_NivelAfiliado': 6, 'U_LF_Autorizacion': '4700100633910', 'U_LF_Mipres': '', 'U_LF_Usuario': 'LAUREN SOFIA PATERNINA ARROYO', 'JournalMemo': 'Escenario dispensación medicar', 'DocDate': '20220131', 'DocumentLines': [{'ItemCode': '7707141301494', 'WarehouseCode': '400', 'AccountCode': '7165950201', 'CostingCode': '4', 'CostingCode2': '400', 'CostingCode3': 'EVPBSSUB', 'BatchNumbers': [{'BatchNumber': 'SE20GH5', 'Quantity': 4}], 'Quantity': 4}]}
        :param url: 'https://vm-hbt-hm34.heinsohncloud.com.co:50000/b1s/v2/DeliveryNotes'
        :return: None
        """
        res = self.post(item, url)
        if value_err := res.get('ERROR'):
            self.info.errs.add(key)
            for csv_item in self.info.data[key]['csv']:
                csv_item['Status'] = f"[SAP] {value_err}"
        else:
            # Si NO hubo ERROR al hacer el POST
            for csv_item in self.info.data[key]['csv']:
                csv_item['Status'] = f"DocEntry: {res.get('DocEntry')}"
            log.info(f"POST Realizado con exito! {key}. DocEntry: {res.get('DocEntry')}")

find_key_by_value = lambda dictionary, value: next((key for key, val in dictionary.items() if val == value), None)
