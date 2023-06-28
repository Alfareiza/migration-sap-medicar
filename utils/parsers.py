import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from core.settings import logger as log
from utils.converters import Csv2Dict
from utils.decorators import logtime
from utils.gdrive.handler_api import GDriveHandler
from utils.resources import set_filename
from utils.sap.connectors import SAPConnect
from utils.sap.manager import SAPData

BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass
class Module:
    name: str  # 'dispensacion', 'factura', etc.
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
            # case 'factura':
            #     self.url = f'{self.BASE_URL}/Invoices'
            #     self.series = 4
            #     self.pk = 'No SSC'  # TODO Confirmar con el csv que va a entregar medicar
            # case 'factura_eventos':
            #     self.url = f'{self.BASE_URL}/Invoices'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 4
            case 'facturacion':
                self.url = {'EVENTO': f'{self.BASE_URL}/Invoices'}
                self.pk = 'No SSC'
                self.series = {'EVENTO': 4}
            # case 'notas_credito':
            #     self.url = f'{self.BASE_URL}/CreditNotes'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 78
            # case 'pago_recibido':
            #     self.url = f'{self.BASE_URL}/IncomingPayments'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 79
            case 'dispensacion':
                self.url = {
                    "CAPITA": f'{self.BASE_URL}/InventoryGenExits',
                    "EVENTO": f'{self.BASE_URL}/DeliveryNotes'
                }
                self.pk = 'No SSC'
                self.series = {'CAPITA': 77, 'EVENTO': 81}
            # case 'entregas_parciales':
            #     self.url = f'{self.BASE_URL}/DeliveryNotes'
            #     self.series = 81
            # case 'transferencia':
            #     self.url = f'{self.BASE_URL}/StockTransfers'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            # case 'entrada_compras':
            #     self.url = f'{self.BASE_URL}/PurchaseDeliveryNotes'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 80
            # case 'inventario_salida':
            #     self.url = f'{self.BASE_URL}/InventoryGenExits'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 82
            # case 'inventario_entrada':
            #     self.url = f'{self.BASE_URL}/InventoryGenEntries'
            #     self.pk = str()  # TODO Confirmar con el csv que va a entregar medicar
            #     self.series = 83
            # case 'ajuste_lote':
            #     self.url = f'{self.BASE_URL}/BatchNumberDetails(lote_number)'  # TODO: Probar endpoint antes de implementar código

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

    def __post_init__(self):
        if self.export:
            self.output_filepath = BASE_DIR / f"{self.module.name}"

    @logtime('')
    def run(self):
        """
        Procesa el csv o bien sea de module.filepath o module.drive
        :return: Csv2Dict
        """
        csv_to_dict = Csv2Dict(self.module.name, self.module.pk, self.module.series, self.module.sap)
        if isinstance(self.input, str):
            self.run_filepath(csv_to_dict)

        elif isinstance(self.input, GDriveHandler):
            self.run_drive(csv_to_dict)

        return csv_to_dict

    def run_filepath(self, csv_to_dict):
        """Procesa el archivo cuando se recibe un csv local."""
        with open(self.input, encoding='utf-8-sig') as csvf:
            csv_reader = csv.DictReader(csvf, delimiter=';')
            self.validate_header(csv_reader.fieldnames)
            csv_to_dict.process(csv_reader)

        if self.export:
            fp = File(csv_to_dict, self.module.name)
            fp.make_csv(f"{self.output_filepath}_only_errors.csv", only_error=True)
            fp.make_json()
            # self.make_csv(csv_to_dict, f"{self.output_filepath}_processed.csv", filter='error')

    def run_drive(self, csv_to_dict):
        """Procesa el archivo cuando se recibe un GDriveHandler."""
        name_folder = self.folder_to_check()
        files = self.discover_files(name_folder)
        sap = SAPConnect(self.module)
        for file in files:
            log.info(f"[CSV] Leyendo {file['name']!r}")
            try:
                csv_reader = self.input.read_csv_file_by_id(file['id'])
                if csv_reader.line_num <= 1:
                    raise Exception('Archivo sin información')
                self.validate_header(csv_reader.fieldnames)
            except Exception as e:
                log.error(e)
                # Envia e-mail informando error en header u otra cosa que se presente.
            else:
                # Procesa el csv, capturando errores y creando payloads.
                csv_to_dict.process(csv_reader)

                # POSTS to SAP
                sap.process(csv_to_dict)

                fp = File(csv_to_dict, self.module.name)

                if self.export:
                    fp.make_json()
                    file_errors = fp.make_csv(f"{self.output_filepath}_only_errors.csv", only_error=True)
                    file_processed = fp.make_csv(f"{self.output_filepath}_processed_all.csv")

                # Crea archivo en Drive con todos los errores
                if csv_to_dict.errs:
                    self.input.send_csv(
                        file_errors,
                        set_filename(file['name'], reason='procesados'),
                        f"{name_folder}_Error"
                    )

                # Crea archivo en Drive con todos los procesados
                self.input.send_csv(
                    file_processed,
                    set_filename(file['name'], reason='procesados'),
                    f"{name_folder}_Procesado"
                )

                self.input.move_file(file['id'], f"{name_folder}_BackUp")

    def discover_files(self, name_folder: str) -> list:
        """Busca los archivos en la carpeta {Modulo}Medicar"""
        files = self.input.get_files_in_folder_by_name(name_folder, ext='csv')
        if not files:
            log.warning(f'No se encontraron archivos en carpeta {name_folder!r}')
            return []
        log.info(f"Archivos reconocidos en carpeta {name_folder!r}:  "
                    f"{', '.join(list(map(lambda f: f['name'], files)))}")
        return files



    def folder_to_check(self) -> str:
        return f"{self.module.name.capitalize()}Medicar"

    def validate_header(self, fieldnames: list) -> None:
        """Valida que os campos do modulo estejam como é esperado"""
        match self.module.name:
            case 'dispensacion':
                dispensacion_fields = {
                    'Fecha Dispensacion', 'SubPlan', 'NIT', 'Plan',
                    'Nro Documento', 'Beneficiario', 'No SSC', 'Categoria Actual',
                    'Numero Autorizacion', 'Mipres', 'Nombre', 'Plu', 'CECO', 'Lote',
                    'Cantidad Dispensada', 'Vlr.Unitario Margen'
                }
                if diff := dispensacion_fields.difference(set(fieldnames)):
                    raise Exception(f"Hacen falta los siguientes campos: {', '.join(diff)}")
            case 'facturacion':
                facturacion_fields = {
                    'Fecha Factura', 'NIT', 'Plan', 'SubPlan', 'Factura',
                    'Nro Documento', 'Beneficiario', 'No SSC', 'Categoria Actual',
                    'Numero Autorizacion', 'Mipres', 'Nombre', 'Plu', 'CECO',
                    'Cantidad Dispensada', 'Vlr.Unitario Margen'
                }
                if diff := facturacion_fields.difference(set(fieldnames)):
                    raise Exception(f"Hacen falta los siguientes campos: {', '.join(diff)}")


@dataclass
class File:
    source: Csv2Dict
    module_name: str

    def make_json(self, jsonfilepath=None) -> None:
        # TODO Se puede dumpar datos de la llave json o csv
        try:
            if not jsonfilepath:
                jsonfilepath = f"{self.module_name}.json"
            with open(jsonfilepath, 'w', encoding='utf-8-sig') as jsonf:
                jsonf.write(json.dumps(self.source.data, indent=4, ensure_ascii=False))
        except Exception as e:
            log.error(f"Creando json con datos: {e}")
        else:
            log.info(f"Archivo creado -> {jsonfilepath}")

    def make_csv(self, csvfilepath: str, only_error=False, only_success=False) -> str:
        log.info(f'Creando csv {only_error=}, {only_success=}')
        rows = self.filter_status(only_error, only_success)
        try:
            if rows:
                fieldnames = rows[0].keys()
                with open(csvfilepath, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
                    writer.writeheader()
                    writer.writerows(rows)
                log.info(f"Archivo creado -> {csvfilepath}")
            else:
                log.info("No fue posible crear csv localmente porque no hubo información a procesar.")
        except Exception as e:
            log.error(f"Creando csv para {self.module_name.capitalize()} al ser procesado: {e}")

        return csvfilepath

    def filter_status(self, only_error, only_success) -> list:
        """
        Filtra el objeto self.source.data com base en si es deseado
        o solo los errores o solo los succes o todos.
        En ningun caso only_error y only_success deberán ser True
        al mismo tiempo.
        :param only_error: True or False
        :param only_success: True or False
        :return: Lista de dicts o luego de haber sido filtrada.
        """
        rows = []
        if only_error:
            for k, v in self.source.data.items():
                if k in self.source.errs or 'sin' in k:
                    rows.extend(v['csv'])
        elif only_success:
            for k in self.source.succss:
                rows.extend(self.source.data[k])
        else:
            for k, v in self.source.data.items():
                rows.extend(v['csv'])
        return rows

        # Old
        # for k, v in self.source.data.items():
        #     if only_error and k in self.source.errs or 'sin' in k:
        #         rows.extend(v['csv'])
        #     elif not only_error:
        #         rows.extend(v['csv'])
        # return rows
