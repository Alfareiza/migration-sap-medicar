import os
from datetime import datetime

from decouple import config
from django.core.management import BaseCommand

from base.models import RegistroMigracion
from core.settings import logger as log
from utils.decorators import logtime
from utils.gdrive.handler_api import GDriveHandler
from utils.interactor_db import crea_registro_migracion, update_estado_finalizado
from utils.parsers import Module
from utils.sap.manager import SAPData


class Command(BaseCommand):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.migracion = None

    @staticmethod
    def migration_proceed():
        last_migration = RegistroMigracion.objects.last()
        return last_migration.estado in ('finalizado', 'error') if last_migration else True

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
        if not self.migration_proceed():
            log.info('No se puede hacer migración, estoy ocupado')
            return

        log.info(f"{' INICIANDO PRUEBAS {} ':▼^70}".format(f"{datetime.now():%T}"))
        self.migracion = crea_registro_migracion()
        log.info(f"{datetime.now():%T}")

        import time
        time.sleep(15 * 60)

        log.info(f"{' FINALIZANDO PRUEBAS {} ':▲^70}".format(f"{datetime.now():%T}"))
        update_estado_finalizado(self.migracion.id)
        return
