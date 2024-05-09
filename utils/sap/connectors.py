from core.settings import logger as log
from utils.decorators import login_required, once_in_interval
from utils.resources import format_number, has_ceco
from utils.sap.manager import SAP


class SAPConnect(SAP):
    def __init__(self, module):
        super().__init__(module)
        self.info = None  # Instancia de clase Csv2Dict
        self.registros = None  # QuerySet con registros insertados en db

    @once_in_interval(2)
    @login_required
    def process(self, csv_to_dict, registros):
        """ Llamado desde pipeline se encarga de definir el tipo de petición
        y ejecutar las peticiones con actualización en DB """
        self.registros = registros
        self.info = csv_to_dict
        method = self.select_method()
        # self.register(method)
        self.gotosap(method)
        log.info(f"[{self.info.name}] {len(self.info.succss)} {method.__name__}s "
                 f"exitosos y {len(self.info.errs)} con error.")

    def select_method(self):
        return self.post if self.info.name != 'ajustes_vencimiento_lote' else self.patch
    # Deprecado Enero/2024
    # @logtime('MASSIVE POSTS')
    # def register(self, method):
    #     with ThreadPoolExecutor(max_workers=4) as executor:
    #         _ = [executor.submit(
    #             self.request_info,  # func
    #             method, key, self.info.data[key]['json'], self.build_url(key)  # args
    #         )
    #             for key in list(self.info.succss)]
    # counter = 0
    # length = len(futures_result)
    # for future in as_completed(futures_result):
    #     counter += 1
    #     # Ex.: [dispensacion] 11.44% 20.615 de 53.050 (4700162): DocEntry: 752066
    #     # Ex.: [dispensacion] 11.41% 20.341 de 53.050 (4751883): 10001153 - Cantidad insuficiente
    #     #      para el artículo 7893884158011 con el lote 123456 en el almacén
    #     log.info(f'[{self.info.name}] {round((counter / length) * 100, 2)}% '
    #              f'{format_number(counter)} de '
    #              f'{format_number(length)} {future.result()}')
    #     futures_result.pop(future)

    def gotosap(self, method):
        """ Ejecuta función request_and_update para todas los payloads """
        length = len(self.info.succss)
        for i, key in enumerate(list(self.info.succss), 1):
            res = self.request_and_update(method, key, self.info.data[key]['json'], self.build_url(key))
            log.info(f'{round((i / length) * 100, 2)}% '
                     f'{format_number(i)} de '
                     f'{format_number(length)} {res}'
                     f" {self.info.data[key]['json'] if '[SAP]' in res else ''}")

    def request_and_update(self, method, key, item, url):
        """Hace petición a API y actualiza resultado en BD """
        if has_ceco(self.info.name, item, '391'):
            msg = 'DocEntry: No aplica'
            res = f"({key}): {msg}"
            self.update_status_csv_column(key, msg)
        else:
            res = self.request_info(method, key, item, url)
        self.update_payloadmigracion(key)
        return res

    def update_payloadmigracion(self, valor_doc: str) -> None:
        """ Actualiza PayloadMigración en BD con base en respuesta después de petición """
        payload = self.registros.get(valor_documento=valor_doc)
        payload.enviado_a_sap = True
        payload.status = self.info.data[valor_doc]['csv'][0]['Status']
        csv_lines = eval(payload.lineas)
        for line in csv_lines:
            line['Status'] = payload.status
        payload.lineas = csv_lines
        payload.save()

    def request_info(self, method: callable, key: str, item: dict, url: str) -> str:
        """
        Realiza un post guarda el registro de si fue exitoso o no.
        :param method: Método a ser llamado en peticiones. Ej. self.post o self.patch
        :param key: '1127507'
        :param item: {'Series': 81, 'U_HBT_Tercero': 'CL901543211',
                    'Comments': 'Escenario Dispensación Medicar', 'U_LF_Plan': 'S',
                    'U_LF_IdAfiliado': '1081818326',
                    'U_LF_NombreAfiliado': 'LAUREN SOFIA PATERNINA ARROYO',
                    'U_LF_Formula': '1127507', 'U_LF_NivelAfiliado': 6,
                    'U_LF_Autorizacion': '4700100633910', 'U_LF_Mipres': '',
                    'U_LF_Usuario': 'LAUREN SOFIA PATERNINA ARROYO',
                    'JournalMemo': 'Escenario dispensación medicar',
                    'DocDate': '20220131', 'DocumentLines':
                     [{'ItemCode': '7707141301494', 'WarehouseCode': '400', 'AccountCode': '7165950201', 'CostingCode': '4', 'CostingCode2': '400', 'CostingCode3': 'EVPBSSUB', 'BatchNumbers': [{'BatchNumber': 'SE20GH5', 'Quantity': 4}], 'Quantity': 4}]}
        :param url: 'https://url-api-sap.com.co:10001/b1s/v8/DeliveryNotes'
        :return: None
        """
        res: dict = method(item, url)
        # res = self.fake_method(item, url)
        if 'ERROR' in res:
            self.info.errs.add(key)
            try:
                self.info.succss.remove(key)
            except Exception:
                ...
            msg = res.get('ERROR')
        else:
            # Si NO hubo ERROR al hacer el POST o PATCH
            if dentry := res.get('DocEntry'):
                ...
            else:
                log.warning(f'{key} No se encontró DocEntry en {res!r}')
            msg = f"DocEntry: {dentry}"
        self.update_status_csv_column(key, msg)
        return f"({key}): {msg}"

    def update_status_csv_column(self, key, msg):
        for csv_item in self.info.data[key]['csv']:
            csv_item['Status'] = msg

    def build_url(self, key):
        """Construye la url a la cual se realizará la petición a la API de SAP."""
        if not isinstance(self.module.series, dict):
            if self.module.name == 'ajustes_vencimiento_lote':
                try:
                    docentry_lote = self.info.data[key]['json'].pop('Series')
                except KeyError as e:
                    log.error(f"ERROR={e}. Lote sin 'Series': {self.info.data[key]}")
                    return self.module.url
                else:
                    return self.module.url.format(docentry_lote)
            return self.module.url

        for k, v in self.module.series.items():
            if v == self.info.data[key]['json']['Series']:
                return self.module.url[k]
