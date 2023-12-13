import os
import signal
import sys

from django.core.management import BaseCommand

from base.models import RegistroMigracion
from core.settings import logger as log
from utils.decorators import logtime, not_on_debug
from utils.gdrive.handler_api import GDriveHandler
from utils.interactor_db import crea_registro_migracion, update_estado_finalizado
from utils.parsers import Module
from utils.sap.manager import SAPData


class Command(BaseCommand):
    help = 'Realiza la migración de determinado modulo'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.migracion = None  # Solamente es creado en producción

    def add_arguments(self, parser):
        parser.add_argument("modulos", nargs="+", type=str)
        parser.add_argument("--filepath", nargs="+", type=str)

    @not_on_debug
    def create_migracion(self):
        self.migracion = crea_registro_migracion()

    @not_on_debug
    def update_estado_para_finalizado(self):
        update_estado_finalizado(self.migracion.id)

    @not_on_debug
    def can_i_proceed(self):
        if not self.migration_proceed():
            log.info(f"{'Migración en ejecución':*^30}")
            return

    @staticmethod
    def migration_proceed():
        last_migration = RegistroMigracion.objects.last()
        return last_migration.estado in ('finalizado',) if last_migration else True

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

        pid = os.getpid()
        signal.signal(signal.SIGTERM, self.handle_sigterm)

        self.can_i_proceed()

        log.info(f"{' INICIANDO MIGRACIÓN {} ':▼^70}".format(pid))
        self.create_migracion()

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
            self.main(*options['modulos'],
                      filepath=options['filepath'][0] if options.get('filepath') else None)

        log.info(f"{' FINALIZANDO MIGRACIÓN {} ':▲^70}".format(pid))
        self.update_estado_para_finalizado()
        return

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
        migracion_id = self.migracion.id if self.migracion else 0
        client = GDriveHandler()
        manager_sap = SAPData()
        for module in args:
            log.info(f'\t===== {module.upper()} ====')
            if dir := kwargs.get('filepath'):
                # Caso sea local
                mdl = Module(name=module, filepath=dir, sap=manager_sap, migracion_id=migracion_id)
            else:
                # Caso sea del drive
                mdl = Module(name=module, drive=client, sap=manager_sap, migracion_id=migracion_id)
            data = mdl.exec_migration(export=True)
            log.info(f'\t===== {module.upper()}  ====')

    def handle_sigterm(self, signum, frame):
        log.warning(f'Abortando migración # {self.migracion.id} con {signum=}')
        sys.exit(1)
