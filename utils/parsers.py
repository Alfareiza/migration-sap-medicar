import csv
import traceback
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
from typing import Optional

from googleapiclient.errors import HttpError

from base.models import PayloadMigracion
from core.settings import logger as log, BASE_DIR, SAP_URL
from utils.converters import Csv2Dict
from utils.decorators import logtime
from utils.gdrive.handler_api import GDriveHandler
from utils.interactor_db import update_estado_error, update_estado_error_sap, DBHandler, update_estado_error_drive, \
    update_estado_error_export, update_estado_error_mail
from utils.mail import (send_mail_due_to_general_error_in_file,
                        send_mail_due_to_impossible_discover_files)
from utils.pipelines import Validate, ProcessCSV, Export, ProcessSAP, Mail, SaveInBD, ExcludeFromDB
from utils.sap.connectors import SAPConnect
from utils.sap.manager import SAPData


@dataclass
class Module:
    name: str  # 'dispensacion', 'factura', 'notas_credito', etc.
    migracion_id: int  # 344, 345, 346.
    filepath: Optional[str] = None  # Ruta del archivo csv con el origen de la información
    drive: Optional[GDriveHandler] = None
    sap: Optional[SAPData] = None
    url: dict = field(init=False)
    series: dict = field(init=False)
    pk: str = field(init=False)

    BASE_URL = SAP_URL

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
            case 'ajustes_entrada_prueba':
                self.url = f'{self.BASE_URL}/InventoryGenEntries'
                self.pk = 'despacho'
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
    pipeline = list = []  # Todo, se está creando una variable llamada list

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
        db = DBHandler(self.module.migracion_id, csv_to_dict.name, csv_to_dict.pk)
        sap = SAPConnect(self.module)
        if isinstance(self.input, (str, PosixPath)):
            self.input = Path(self.input) if isinstance(self.input, str) else self.input
            db.fname = self.input.stem
            self.pipeline = [Validate, ProcessCSV, SaveInBD, ProcessSAP, Export, Mail, ExcludeFromDB]
            self.run_filepath(csv_to_dict, db, sap)

        elif isinstance(self.input, GDriveHandler):
            self.export = True
            self.pipeline = [Validate, ProcessCSV, SaveInBD, ProcessSAP, Export, Mail, ExcludeFromDB]
            self.run_drive(csv_to_dict, db, sap)

        return csv_to_dict

    def run_filepath(self, csv_to_dict, db, sap):
        """Procesa el archivo cuando se recibe un csv local."""
        try:
            records = PayloadMigracion.objects.filter(nombre_archivo=self.input.stem, modulo=self.module.name)
            if not records:
                with open(self.input, encoding='utf-8-sig') as csvf:
                    csv_reader = csv.DictReader(csvf, delimiter=';')
                    for self.proc in self.pipeline:
                        self.proc().run(csv_to_dict=csv_to_dict, reader=csv_reader, db=db,
                                        parser=self, filename=db.fname, sap=sap)
            else:
                self.strategy_records_already_exists(records, csv_to_dict, sap, db)

        except Exception as e:
            update_estado_error(self.module.migracion_id)
            send_mail_due_to_general_error_in_file(f"{db.fname}.csv", e,
                                                   traceback.format_exc(),
                                                   self.pipeline.index(self.proc) + 1,
                                                   self.proc or '',
                                                   list(self.pipeline)
                                                   )
            raise

    def run_drive(self, csv_to_dict, db, sap):
        """Procesa el archivo cuando se recibe un GDriveHandler."""
        name_folder = self.folder_to_check()
        self.discover_files(name_folder)
        for i, file in enumerate(self.input.files, 1):
            try:
                records = PayloadMigracion.objects.filter(nombre_archivo=file['name'][:-4],
                                                          modulo=self.module.name)
                db.fname = file['name'][:-4]
                if not records:
                    log.info(f"[CSV] Leyendo {i} de {len(self.input.files)} {file['name']!r}")
                    csv_reader = self.input.read_csv_file_by_id(file['id'])
                    for self.proc in self.pipeline:
                        self.proc().run(csv_to_dict=csv_to_dict, reader=csv_reader,
                                        parser=self, sap=sap, file=file, db=db,
                                        name_folder=name_folder, filename=db.fname)

                    csv_to_dict.clear_data()
                else:
                    self.strategy_records_already_exists(records, csv_to_dict, sap, db,
                                                         file=file, name_folder=name_folder)
            except Exception as e:
                send_mail_due_to_general_error_in_file(file['name'], e, traceback.format_exc(),
                                                       self.pipeline.index(self.proc) + 1,
                                                       self.proc or '',
                                                       list(self.pipeline))
                self.strategy_post_error(self.proc.__name__)
                raise

    def strategy_records_already_exists(self, records, csv_to_dict, sap, db, file=None, name_folder=None):
        db.registros = records.filter(enviado_a_sap=False)
        csv_to_dict.load_data_from_db(db.registros)
        payloads = records.filter(enviado_a_sap=True)

        log.info(f'[{csv_to_dict.name}] {len(records)} Migraciones encontradas. {len(payloads)}'
                 f' fueron enviados a sap y {len(csv_to_dict.data)} no')

        # Ejecutará el pipeline desde el paso después de SaveInBD
        for self.proc in self.pipeline[self.pipeline.index(SaveInBD) + 1:]:
            self.proc().run(csv_to_dict=csv_to_dict, db=db, file=file, name_folder=name_folder,
                            parser=self, filename=db.fname, sap=sap,
                            payloads_previously_sent=payloads)
        csv_to_dict.clear_data()

    def strategy_post_error(self, proc_name):
        """
        Ejecuta algo a partir del nombre del step.
        Esta función es ejecutada cuando suceda un error durante
        la ejecución de uno de los pasos del pipeline.
        """
        match proc_name:
            case 'Validate':
                update_estado_error(self.module.migracion_id)
            case 'ProcessCSV':
                ...
            case 'ProcessSAP':
                update_estado_error_sap(self.module.migracion_id)
            case 'Export':
                update_estado_error_export(self.module.migracion_id)
            case 'Mail':
                update_estado_error_mail(self.module.migracion_id)

    def discover_files(self, name_folder: str) -> None:
        """Busca los archivos en la carpeta {Modulo}Medicar y los carga
        en la variable self.input.files. Siendo esta una lista de diccionarios
        donde cada diccionario representa un archivo. El primero de la lista
        es el archivo más viejo"""
        try:
            files = self.input.get_files_in_folder_by_name(name_folder, ext='csv')
        except HttpError as e:
            log.warning(f'Error {e} al buscar archivos en carpeta {name_folder!r}')
            send_mail_due_to_impossible_discover_files(name_folder, traceback.format_exc())
            update_estado_error_drive(self.module.migracion_id)
        else:
            if not files:
                log.warning(f'No se encontraron archivos en carpeta {name_folder!r}')
            else:
                log.info(f"Archivos reconocidos en carpeta {name_folder!r}:  "
                         f"{', '.join(list(map(lambda f: f['name'], files)))}")
            self.input.files = files

    def folder_to_check(self) -> str:
        if self.module.name not in ('ajustes_entrada', 'ajustes_entrada_prueba', 'ajustes_salida', 'notas_credito',
                                    'ajustes_vencimiento_lote', 'pagos_recibidos', 'pagos_recibidos',
                                    'dispensaciones_anuladas'):
            return f"{self.module.name.capitalize()}Medicar"
        words = self.module.name.split('_')
        base_word = ' '.join(words).title().replace(' ', '')
        return f"{base_word}Medicar"
