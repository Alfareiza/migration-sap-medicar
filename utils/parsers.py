import csv
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
from typing import Optional

from googleapiclient.errors import HttpError
from django.conf import settings

from base.models import PayloadMigracion
from core.settings import logger as log, BASE_DIR, SAP_URL
from utils.converters import Csv2Dict
from utils.decorators import logtime
from utils.resources import format_number as fn
from utils.gdrive.handler_api import GDriveHandler
from utils.interactor_db import (
    DBHandler,
    update_estado_error,
    update_estado_error_drive,
    update_estado_error_export,
    update_estado_error_mail,
    update_estado_error_sap
)
from utils.mail import (
    send_mail_due_to_general_error_in_file,
    send_mail_due_to_impossible_discover_files
)
from utils.pipelines import (
    ExcludeFromDB,
    Export,
    Mail,
    ProcessCSV,
    ProcessSAP,
    SaveInBD,
    Validate,
)
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
            case settings.FACTURACION_NAME:
                self.url = {'EVENTO': f'{self.BASE_URL}/Invoices'}
                self.pk = 'NroSSC'
                self.series = {'EVENTO': 4}
            case settings.NOTAS_CREDITO_NAME:
                self.url = f'{self.BASE_URL}/CreditNotes'
                self.pk = 'NroSSC'
                self.series = 78
            case settings.PAGOS_RECIBIDOS_NAME:
                self.url = f'{self.BASE_URL}/IncomingPayments'
                self.pk = 'NIT'  # Preguntar a Elias cual es el pk
                self.series = 79
            case settings.DISPENSACION_NAME:
                self.url = {
                    "CAPITA": f'{self.BASE_URL}/InventoryGenExits',
                    "EVENTO": f'{self.BASE_URL}/DeliveryNotes'
                }
                self.pk = 'NroSSC'
                self.series = {'CAPITA': 77, 'EVENTO': 81}
            case settings.TRASLADOS_NAME:
                self.url = f'{self.BASE_URL}/StockTransfers'
                self.pk = 'NroDocumento'
                self.series = None  # No usa
            case settings.COMPRAS_NAME:
                self.url = f'{self.BASE_URL}/PurchaseDeliveryNotes'
                self.pk = 'NroDocumento'
                self.series = 80
            case settings.AJUSTES_SALIDA_NAME:
                self.url = f'{self.BASE_URL}/InventoryGenExits'
                self.pk = 'NroDocumento'
                self.series = 82
            case settings.AJUSTES_ENTRADA_NAME:
                self.url = f'{self.BASE_URL}/InventoryGenEntries'
                self.pk = 'NroDocumento'
                self.series = 83
            case settings.AJUSTES_ENTRADA_PRUEBA_NAME:
                self.url = f'{self.BASE_URL}/InventoryGenEntries'
                self.pk = 'despacho'
                self.series = 83
            case settings.DISPENSACIONES_ANULADAS_NAME:
                self.url = f'{self.BASE_URL}/InventoryGenEntries'
                self.pk = 'NroSSC'
                self.series = 83
            case settings.AJUSTES_LOTE_NAME:
                self.url = f'{self.BASE_URL}/BatchNumberDetails({{}})'  # TODO: Probar endpoint antes de implementar código
                self.series = None
                self.pk = 'Lote'

    def exec_migration(self, tanda: str = '') -> Csv2Dict:
        """
        Crea Parser que es responsable por:
            - Detecta archivos a procesar
            - Procesa archivo(s).
                - Lee archivo(s) del drive.
                - Valida header.
                - Crea jsons internamente (payloads).
                - Crea archivo con todos los logs en carpeta del drive.
                - Crea archivo con errores en carpeta del drive.
        :param tanda: Indica cual es la tanda de ejecución del programa
                 Ej.: '1RA' o '2DA'
        :return: Objeto Csv2Dict con la información procesada.
        """
        parser = Parser(self, self.filepath or self.drive, tanda)
        return parser.run()


@dataclass
class Parser:
    module: Module
    input: str or GDriveHandler
    tanda: str
    output_filepath: str = ''

    def __post_init__(self):
        self.pipeline = []
        self.output_filepath = BASE_DIR / f"{self.module.name}"
        self.set_pipeline()

        # Deprecated 20/Jan/24
        # elif isinstance(self.input, (str, PosixPath)):
        # Si no se exportan los archivos, entonces no se debe enviar correo
        # del self.pipeline[-1]

    def set_pipeline(self):
        """ Define cual va a ser el pipeline con base en param. """
        if self.tanda == '1RA':
            self.pipeline = (Validate, ProcessCSV, SaveInBD, ProcessSAP)
        elif self.tanda == '2DA':
            self.pipeline = (ProcessSAP, Export, Mail, ExcludeFromDB)
        elif self.tanda == 'TEST':
            self.pipeline = (Validate, ProcessCSV, SaveInBD, ExcludeFromDB)
        else:
            raise Exception(f'Tanda no ha sido definida. Recibido: {self.tanda!r}')

    @logtime('')
    def run(self):
        """
        Procesa el csv o bien sea de module.filepath o module.drive.
        Teniendo en cuenta la tanda, ejecuta un pipeline dado para esa tanda.
        :return: Instance de Csv2Dict luego de haber sido procesado
        """
        csv_to_dict = Csv2Dict(self.module.name, self.module.pk, self.module.series, self.module.sap)
        db = DBHandler(self.module.migracion_id, csv_to_dict.name, csv_to_dict.pk)
        sap = SAPConnect(self.module)

        if isinstance(self.input, (str, PosixPath)):
            self.input = Path(self.input) if isinstance(self.input, str) else self.input
            db.fname = self.input.stem
            self.run_filepath(csv_to_dict, db, sap)

        elif isinstance(self.input, GDriveHandler):
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
                self.existing_records(records, csv_to_dict, sap, db)

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
                if not records and self.tanda == '1RA':
                    log.info(f"[CSV] Leyendo {i} de {len(self.input.files)} {file['name']!r}")
                    csv_reader = self.input.read_csv_file_by_id(file['id'])
                    for self.proc in self.pipeline:
                        self.proc().run(csv_to_dict=csv_to_dict, reader=csv_reader,
                                        parser=self, sap=sap, file=file, db=db,
                                        name_folder=name_folder, filename=db.fname)
                        time.sleep(3)
                    csv_to_dict.clear_data()
                elif records:
                    self.existing_records(records, csv_to_dict, sap, db,
                                          file=file, name_folder=name_folder)
                else:
                    log.info(f"[{self.module.name}] archivo {db.fname} creado durante ejecución de"
                             f" primera tanda, se puede procesar en segunda tanda")
            except Exception as e:
                send_mail_due_to_general_error_in_file(file['name'], e, traceback.format_exc(),
                                                       self.pipeline.index(self.proc) + 1,
                                                       self.proc or '',
                                                       list(self.pipeline))
                self.strategy_post_error(self.proc.__name__)
                raise

    def existing_records(self, records, csv_to_dict, sap, db, file=None, name_folder=None):
        # Si hay records del archivo y algunos no se han enviado a sap, entonces
        # Es por que se cayó la última migración
        db.records = records.filter(enviado_a_sap=False)

        # Esos registros que no han sido enviado a sap, los monta en el csv_to_dict
        # Porque cuando llegue el paso de procesar la base de datos
        # el va a enviar a sap los que tenga que enviar (succss)
        csv_to_dict.load_data_from_db(db.records)

        already_sent = records.filter(enviado_a_sap=True)
        if self.tanda == '1RA':
            self.pipeline = (ProcessSAP,)

        log.info("[{}] Archivo {}.csv. {} Migraciones encontradas. "
                 "Enviadas a SAP: {}, Por enviar: {}."
                 " De las cuales {} tienen error mientras que {} no tienen error."
                 .format(csv_to_dict.name, db.fname,
                         fn(len(records)),
                         fn(len(already_sent)),
                         fn(len(csv_to_dict.data)),
                         fn(len(csv_to_dict.errs)),
                         fn(len(csv_to_dict.succss))
                         )
                 )

        # Ejecutará el pipeline desde el paso después de SaveInBD
        for self.proc in self.pipeline:
            self.proc().run(csv_to_dict=csv_to_dict, db=db, file=file, name_folder=name_folder,
                            parser=self, filename=db.fname, sap=sap,
                            payloads_previously_sent=already_sent)
            time.sleep(3)
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
        """Crea el nombre de la carpeta a ser buscada en Google Drive"""
        if self.module.name not in ('ajustes_entrada', 'ajustes_entrada_prueba', 'ajustes_salida', 'notas_credito',
                                    'ajustes_vencimiento_lote', 'pagos_recibidos', 'pagos_recibidos',
                                    'dispensaciones_anuladas'):
            return f"{self.module.name.capitalize()}Medicar"  # Compras
        words = self.module.name.split('_')
        base_word = ' '.join(words).title().replace(' ', '')
        return f"{base_word}Medicar"
