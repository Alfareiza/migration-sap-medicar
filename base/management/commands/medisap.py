import os

from decouple import config
from django.core.management import BaseCommand

from core.settings import logger as log
from utils.decorators import logtime
from utils.gdrive.handler_api import GDriveHandler
from utils.parsers import Module
from utils.sap.manager import SAPData


class Command(BaseCommand):
    help = 'Realiza la migración de detemrinado modulo'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument("modulos", nargs="+", type=str)
        parser.add_argument("--filepath", nargs="+", type=str)

    @logtime('MIGRATION BOT')
    def handle(self, *args, **options):
        """
        Main function who execute the command which will be called executing:
        python manage.py medicar_to_sap foo from the CLI.
        Ex.:
            - python manage.py medisap dispensacion --filepath=/Users/jane/Downloads/file.csv
            - python manage.py medisap dispensacion
            - python manage.py medisap todos
        :param args: ...
        :param options: A dict like this {'verbosity': 1, 'settings': None,
         'pythonpath': None, 'traceback': False, 'no_color': False,
         'force_color': False, 'skip_checks': False, 'modulos': ['foo']}
        """
        if "TASK_STATUS" in os.environ:
            log.info(f"Como estoy {os.environ['TASK_STATUS']}, no voy a hacer nada")
            return None

        os.environ["TASK_STATUS"] = 'ocupado'
        log.info(f'status actual es {os.environ.get("TASK_STATUS")}')

        log.info(f"{' INICIANDO MIGRACIÓN ':▼^70}")
        if options['modulos'] == ['todos']:
            self.main(
                'compras',
                'traslados',
                'ajustes_entrada_prueba',
                'ajustes_entrada',
                'ajustes_salida',
                'ajustes_vencimiento_lote',
                'dispensacion',
                'dispensaciones_anuladas',
                'facturacion',
                'notas_credito',
                'pagos_recibidos',
            )
        else:
            self.main(*options['modulos'], filepath=options['filepath'][0] if options.get('filepath') else None)

        log.info(f"{' MIGRACIÓN FINALIZADA ':▲^70}")

    def main(self, *args, **kwargs):
        """
        Execute the migration of information from a csv which
        may be in Gdrive or local.
        :param args: List of modules in SAP.
                    Ex.:
                        - ('dispensacion',)
                        - ('ajustes_entrada', 'ajustes_salida')
        :param kwargs: Might be {'filepath': 'path_of_the_file.csv'}
        """
        import time
        time.sleep(720)
        # client = GDriveHandler()
        # manager_sap = SAPData()
        # for module in args:
        #     log.info(f'\t===== {module.upper()} ====')
        #     if dir := kwargs.get('filepath'):
        #         mdl = Module(name=module, filepath=dir, sap=manager_sap)  # Caso sea local
        #     else:
        #         mdl = Module(name=module, drive=client, sap=manager_sap)  # Caso sea del drive
        #     data = mdl.exec_migration(export=True)
        #     log.info(f'\t===== {module.upper()}  ====')

        # log.info("Cambiando estado de migración para 'LIBRE'")
        # os.environ["TASK_STATUS"] = 'libre'
        log.info(f'status final es {os.environ["TASK_STATUS"]!r}')
        try:
            del os.environ['TASK_STATUS']
        except Exception as e:
            log.error('No fue posible eliminar TASK_STATUS de las variables de ambiente')