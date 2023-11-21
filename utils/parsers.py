import csv
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
from typing import Optional

from core.settings import logger as log, BASE_DIR
from utils.converters import Csv2Dict
from utils.decorators import logtime
from utils.gdrive.handler_api import GDriveHandler
from utils.pipelines import Validate, ProcessCSV, Export, ProcessSAP, Mail
from utils.sap.connectors import SAPConnect
from utils.sap.manager import SAPData


@dataclass
class Module:
    name: str  # 'dispensacion', 'factura', 'notas_credito', etc.
    filepath: Optional[str] = None  # Ruta del archivo csv con el origen de la información
    drive: Optional[GDriveHandler] = None
    sap: Optional[SAPData] = None
    url: dict = field(init=False)
    series: dict = field(init=False)
    pk: str = field(init=False)

    BASE_URL = 'https://vm-hbt-hm34.heinsohncloud.com.co:50000/b1s/v2'

    # BASE_URL = 'https://any.com.co:50000/b1s/v2'

    def __post_init__(self):
        if self.filepath:
            if not Path(self.filepath).exists():
                raise FileNotFoundError(f"File not found -> {self.filepath}")
        if self.filepath and self.drive:
            # Valida que no sean recibidos estos dos atributos juntos.
            raise Exception('It\'s no possible to configure filepath and '
                            'drive object at the same time.')

        match self.name.lower():
            # case 'factura_eventos':
            #     self.url = f'{self.BASE_URL}/Invoices'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 4
            case 'facturacion':
                self.url = {'EVENTO': f'{self.BASE_URL}/Invoices'}
                self.pk = 'NroSSC'
                self.series = {'EVENTO': 4}
            case 'notas_credito':
                self.url = f'{self.BASE_URL}/CreditNotes'
                self.pk = 'NroSSC'
                self.series = 78
            case 'pagos_recibidos':
                self.url = f'{self.BASE_URL}/IncomingPayments'
                self.pk = 'NIT'  # Preguntar a Elias cual es el pk
                self.series = 79
            case 'dispensacion':
                self.url = {
                    "CAPITA": f'{self.BASE_URL}/InventoryGenExits',
                    "EVENTO": f'{self.BASE_URL}/DeliveryNotes'
                }
                self.pk = 'NroSSC'
                self.series = {'CAPITA': 77, 'EVENTO': 81}
            case 'traslados':
                self.url = f'{self.BASE_URL}/StockTransfers'
                self.pk = 'NroDocumento'
                self.series = None  # No usa
            case 'compras':
                self.url = f'{self.BASE_URL}/PurchaseDeliveryNotes'
                self.pk = 'NroDocumento'
                self.series = 80
            case 'ajustes_salida':
                self.url = f'{self.BASE_URL}/InventoryGenExits'
                self.pk = 'NroDocumento'
                self.series = 82
            case 'ajustes_entrada':
                self.url = f'{self.BASE_URL}/InventoryGenEntries'
                self.pk = 'NroDocumento'
                self.series = 83
            case 'dispensaciones_anuladas':
                self.url = f'{self.BASE_URL}/InventoryGenEntries'
                self.pk = 'NroSSC'
                self.series = 83
            case 'ajustes_vencimiento_lote':
                self.url = f'{self.BASE_URL}/BatchNumberDetails({{}})'  # TODO: Probar endpoint antes de implementar código
                self.series = None
                self.pk = 'Lote'

    def exec_migration(self, export: bool = False) -> Csv2Dict:
        """
        Crea Parser que es responsable por:
            - Detecta archivos a procesar
            - Procesa archivo(s).
                - Lee archivo(s) del drive.
                - Valida header.
                - Crea jsons internamente (payloads).
                - Crea archivo con todos los logs en carpeta del drive.
                - Crea archivo con errores en carpeta del drive.
                - (opcional) exporta localmente el mismo csv con errores.
        :param export: Define si habrá exportación local o no de la info
                       procesada en json y csv.
        :return: Objeto Csv2Dict con la información procesada.
        """
        parser = Parser(self, self.filepath or self.drive, export=export)
        return parser.run()


@dataclass
class Parser:
    module: Module
    input: str or GDriveHandler
    export: bool = False
    output_filepath: str = ''
    pipeline = list = [Validate, ProcessCSV, Export,
                       Mail
                       ]

    def __post_init__(self):
        if self.export:
            self.output_filepath = BASE_DIR / f"{self.module.name}"
        elif isinstance(self.input, (str, PosixPath)):
            # Si no se exportan los archivos, entonces no se debe enviar correo
            del self.pipeline[-1]


    @logtime('')
    def run(self):
        """
        Procesa el csv o bien sea de module.filepath o module.drive
        :return: Instance de Csv2Dict luego de haber sido procesado
        """
        csv_to_dict = Csv2Dict(self.module.name, self.module.pk, self.module.series, self.module.sap)
        if isinstance(self.input, (str, PosixPath)):
            self.run_filepath(csv_to_dict)

        elif isinstance(self.input, GDriveHandler):
            self.export = True
            self.pipeline.insert(-2, ProcessSAP)  # PARA VALIDACION DE CSVS DEL DRIVE SIN PROCESAMIENTO EN SAP, COMENTAR ESTA LINEA
            self.run_drive(csv_to_dict)

        return csv_to_dict

    def run_filepath(self, csv_to_dict):
        """Procesa el archivo cuando se recibe un csv local."""
        with open(self.input, encoding='utf-8-sig') as csvf:
            csv_reader = csv.DictReader(csvf, delimiter=';')
            for proc in self.pipeline:
                proc().run(csv_to_dict=csv_to_dict, reader=csv_reader, parser=self)
            # log.error(f"{proc.__str__(proc)} > {e}")

    def run_drive(self, csv_to_dict):
        """Procesa el archivo cuando se recibe un GDriveHandler."""
        name_folder = self.folder_to_check()
        self.discover_files(name_folder)
        sap = SAPConnect(self.module)
        for file in self.input.files:
            log.info(f"[CSV] Leyendo {file['name']!r}")
            csv_reader = self.input.read_csv_file_by_id(file['id'])
            for proc in self.pipeline:
                proc().run(csv_to_dict=csv_to_dict, reader=csv_reader,
                           parser=self, sap=sap, file=file,
                           name_folder=name_folder)

    def discover_files(self, name_folder: str) -> list:
        """Busca los archivos en la carpeta {Modulo}Medicar"""
        files = self.input.get_files_in_folder_by_name(name_folder, ext='csv')
        if not files:
            log.warning(f'No se encontraron archivos en carpeta {name_folder!r}')
        log.info(f"Archivos reconocidos en carpeta {name_folder!r}:  "
                 f"{', '.join(list(map(lambda f: f['name'], files)))}")
        self.input.files = files

    def folder_to_check(self) -> str:
        if self.module.name not in ('ajustes_entrada', 'ajustes_salida', 'notas_credito',
                                    'ajustes_vencimiento_lote', 'pagos_recibidos', 'pagos_recibidos',
                                    'dispensaciones_anuladas'):
            return f"{self.module.name.capitalize()}Medicar"
        words = self.module.name.split('_')
        base_word = ' '.join(words).title().replace(' ', '')
        return f"{base_word}Medicar"
