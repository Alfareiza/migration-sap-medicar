import logging

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
        :param args: ...
        :param options: A dict like this {'verbosity': 1, 'settings': None,
         'pythonpath': None, 'traceback': False, 'no_color': False,
         'force_color': False, 'skip_checks': False, 'modulos': ['foo']}
        """
        log.info('===== INICIANDO MIGRACIÓN ====')
        if options.get('todos'):
            self.main(
                'inventario_entrada',
                'transferencia',
                'entrada_comprasdispensacion',
                'dispensacion'
                'facturacion',
            )
        else:
            self.main(*options['modulos'], filepath=options['filepath'][0] if options.get('filepath') else None)

        log.info('===== MIGRACIÓN FINALIZADA ====')

    def main(self, *args, **kwargs):
        """
        Execute the migration of information from a csv which
        may be in Gdrive or locally.
        :param args: List of modules in SAP.
        :param kwargs: Might be {'filepath': 'path_of_the_file.csv'}
        """
        client = GDriveHandler()
        manager_sap = SAPData()
        for module in args:
            log.info(f'\t===== {module.upper()} ====')
            if dir := kwargs.get('filepath'):
                mdl = Module(name=module, filepath=dir, sap=manager_sap)  # Caso sea local
            else:
                mdl = Module(name=module, drive=client, sap=manager_sap)  # Caso sea del drive
            data = mdl.exec_migration(export=True)
            log.info(f'\t===== {module.upper()}  ====')
