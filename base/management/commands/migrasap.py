import os
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler

os.environ['DJANGO_SETTINGS_MODULE'] = 'core.settings'
import django

django.setup()
from base.models import RegistroMigracion
from core.settings import logger as log, DEBUG
from utils.decorators import logtime, not_on_debug
from utils.gdrive.handler_api import GDriveHandler
from utils.interactor_db import crea_registro_migracion, update_estado_finalizado, update_estado_error_heroku
from utils.parsers import Module
from utils.sap.manager import SAPData

global migracion_id


class Command:
    help = 'Realiza la migración de determinado modulo'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tanda = None
        self.migracion = None
        self.last_migration = 0

    def add_arguments(self, parser):
        parser.add_argument("modulos", nargs="+", type=str)
        parser.add_argument("--filepath", nargs="+", type=str)

    @not_on_debug
    def create_migracion(self):
        self.migracion = crea_registro_migracion()
        global migracion_id
        migracion_id = self.migracion.id

    @not_on_debug
    def update_estado_para_finalizado(self):
        update_estado_finalizado(self.migracion.id)

    def migration_proceed(self):
        self.last_migration = RegistroMigracion.objects.last()
        return self.last_migration.estado in ('finalizado', 'heroku') if self.last_migration else True

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

        if not DEBUG and not self.migration_proceed():
            log.info(f"{' migración en estado: {} ':*^40}".format(self.last_migration))
            return

        self.create_migracion()

        self.tanda = options['tanda'] or ''

        log.info(f"{' INICIANDO {} TANDA - MIGRACIÓN # {} ':▼^80}".format(self.tanda, self.migracion.id))

        if options['modulos'] == ['todos']:
            self.main(
                'compras',
                'traslados',
                # 'ajustes_entrada_prueba',
                'ajustes_entrada',
                'ajustes_salida',
                'ajustes_vencimiento_lote',
                'dispensacion',
                'dispensaciones_anuladas',
                'facturacion',
                'notas_credito',
                'pagos_recibidos',
                tanda=self.tanda
            )
        else:
            self.main(*options['modulos'],
                      filepath=options['filepath'][0] if options.get('filepath') else None,
                      tanda=self.tanda)

        log.info(f"{' FINALIZANDO {} TANDA - MIGRACIÓN # {} ':▲^80}".format(self.tanda, self.migracion.id))

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
        # migracion_id = self.migracion.id if self.migracion else 0
        client = GDriveHandler()
        manager_sap = SAPData()
        for module in args:
            log.info(f'{f" INICIO {module.upper()} {self.tanda} TANDA ":=^80}')
            if dir := kwargs.get('filepath'):
                # Caso sea local
                mdl = Module(name=module, filepath=dir, sap=manager_sap, migracion_id=migracion_id)
            else:
                # Caso sea del drive
                mdl = Module(name=module, drive=client, sap=manager_sap, migracion_id=migracion_id)
            data = mdl.exec_migration(tanda=kwargs.get('tanda'))
            log.info(f'{f" FIN {module.upper()} {self.tanda} TANDA ":=^80}')


def handle_sigterm(*args):
    [log.warning(f"Abortando migración con arg {i}->{arg}") for i, arg in enumerate(args, 1)]
    if args and args[0] == 15 or args[0] == '15':
        update_estado_error_heroku(migracion_id)
    sys.exit(1)


sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes=10)
def timed_job():
    c = Command()
    c.handle(modulos=['todos'], tanda='1RA')
    log.info(10 * '  ')
    log.info(10 * '  ')
    c.handle(modulos=['todos'], tanda='2DA')


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    sched.start()
