import os
from datetime import datetime

from decouple import config
from django.core.management import BaseCommand

from core.settings import logger as log
from utils.decorators import logtime
from utils.gdrive.handler_api import GDriveHandler
from utils.parsers import Module
from utils.sap.manager import SAPData


class Command(BaseCommand):

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
        # if "TASK_STATUS" in os.environ:
        #     log.info(f"Como estoy {os.environ['TASK_STATUS']}, no voy a hacer nada")
        #     return None
        #
        # os.environ["TASK_STATUS"] = 'ocupado'
        # log.info(f'status actual es {os.environ.get("TASK_STATUS")}')

        log.info(f"{' INICIANDO PRUEBAS {} ':▼^70}".format(f"{datetime.now():%T}"))
        log.info(f"{datetime.now():%T}")
        import time
        time.sleep(15 * 60)
        log.info(f"{' FINALIZANDO PRUEBAS {} ':▲^70}".format(f"{datetime.now():%T}"))
        return
