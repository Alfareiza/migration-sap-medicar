import datetime
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm

from core.settings import logger as log
from utils.decorators import logtime, login_required, once_in_interval
from utils.sap.manager import SAP


class SAPConnect(SAP):
    def __init__(self, module):
        super().__init__(module)
        self.info = None  # Instancia de clase Csv2Dict

    @once_in_interval(2)
    @login_required
    def process(self, csv_to_dict):
        self.info = csv_to_dict
        method = self.post if self.info.name != 'ajustes_vencimiento_lote' else self.patch
        self.register(method)
        # self.register_sync(method)
        log.info(f"{self.info.name} {len(self.info.succss)} {method.__name__}s exitosos y {len(self.info.errs)} con error.")

    @logtime('MASSIVE POSTS')
    def register(self, method):
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(
                self.request_info,  # func
                method, key, self.info.data[key]['json'], self.build_url(key)  # args
            )
                for key in list(self.info.succss)]

            counter = 0
            length = len(futures)
            for future in as_completed(futures):
                counter += 1
                log.info(f'[{self.info.name}] {counter} de {length} {future.result()}')

    def register_sync(self, method):
        for i, key in enumerate(list(self.info.succss), 1):
            log.info(f'{method.__name__}ing {i} {key}')
            self.request_info(method, key, self.info.data[key]['json'], self.build_url(key))

    def request_info(self, method, key, item, url):
        """
        Realiza un post guarda el registro de si fue exitoso o no.
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
        :param url: 'https://vm-hbt-hm34.heinsohncloud.com.co:50000/b1s/v2/DeliveryNotes'
        :return: None
        """
        res = method(item, url)
        if value_err := res.get('ERROR'):
            self.info.errs.add(key)
            try:
                self.info.succss.remove(key)
            except Exception:
                ...
            msg = value_err
            for csv_item in self.info.data[key]['csv']:
                csv_item['Status'] = f"[SAP] {value_err}"
        else:
            # Si NO hubo ERROR al hacer el POST o PATCH
            msg = f"DocEntry: {res.get('DocEntry')}"
            for csv_item in self.info.data[key]['csv']:
                csv_item['Status'] = msg
            # log.info(f"[{self.info.name}] {method.__name__.upper()} Realizado con exito! {key}. DocEntry: {res.get('DocEntry')}")
        return f"({key}): {msg}"

    def build_url(self, key):
        """Contruye la url a la cual se realizará la petición a la API de SAP."""
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


import subprocess

def get_highest_ram_process():
    try:
        # Run the ps command to get process information
        ps_output = subprocess.check_output(["ps", "-eo", "pid,%mem,cmd", "--sort=-%mem"]).decode("utf-8")

        # Get the first line (header) and the second line (process with highest memory usage)
        lines = ps_output.strip().split('\n')
        header = lines[0]
        highest_ram_process = lines[1]

        # Extract relevant information from the header and highest_ram_process
        header_fields = header.split()
        process_info = highest_ram_process.split()

        # Print information about the process with the highest memory usage
        pid_index = header_fields.index("PID")
        mem_index = header_fields.index("%MEM")
        cmd_index = header_fields.index("CMD")

        pid = process_info[pid_index]
        mem_usage = process_info[mem_index]
        cmd = process_info[cmd_index]

        log.info(f"Process -> PID: {pid}, RAM Usage: {mem_usage}%, Command: {cmd}")
    except subprocess.CalledProcessError as e:
        log.errord(f"Error: {e}")

