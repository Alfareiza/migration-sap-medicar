import os
import signal
import sys
from datetime import datetime

import os

os.environ['DJANGO_SETTINGS_MODULE'] = 'core.settings'
import django;

django.setup()
from apscheduler.schedulers.background import BlockingScheduler
from django.core.management import BaseCommand

from base.models import RegistroMigracion
from core.settings import logger as log
from utils.interactor_db import crea_registro_migracion, update_estado_finalizado


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
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        if not self.migration_proceed():
            log.info('No se puede hacer migración, estoy ocupado')
            return
        hora_inicio = f"{datetime.now():%T}"

        log.info(f"{' INICIANDO PRUEBAS {} ':▼^70}".format(hora_inicio))
        self.migracion = crea_registro_migracion()
        log.info(f"{datetime.now():%T}")

        import time
        for self.i in range(1, 8):
            time.sleep(600)
            log.info(f'... van {self.i}0 minutos')

        log.info(f"{' FINALIZANDO PRUEBAS INICIADAS A LAS {} ':▲^70}".format(hora_inicio))
        update_estado_finalizado(self.migracion.id)
        return

    def handle_sigterm(self, signum, frame):
        log.warning(f'Abortando pruebas  con {signum=}. Iban {self.i}0 minutos')
        update_estado_finalizado(self.migracion.id)
        sys.exit(1)


sched = BlockingScheduler()


def handle_sigterm(self, signum, frame):
    log.warning(f'Abortando pruebas  con {signum=}. Iban {self.i}0 minutos')
    update_estado_finalizado(self.migracion.id)
    sys.exit(1)


def migration_proceed():
    last_migration = RegistroMigracion.objects.last()
    return last_migration.estado in ('finalizado', 'error') if last_migration else True


@sched.scheduled_job('interval', minutes=10)
def timed_job():
    # signal.signal(signal.SIGTERM, handle_sigterm)
    if not migration_proceed():
        log.info('No se pueden hacer pruebas, estoy ocupado')
        return
    hora_inicio = f"{datetime.now():%T}"

    log.info(f"{' INICIANDO PRUEBAS {} ':▼^70}".format(hora_inicio))
    migracion = crea_registro_migracion()
    log.info(f"{' Ejecutando pruebas {} ':.>70}".format(hora_inicio))

    import time
    for i in range(1, 10):
        time.sleep(600)
        log.info(f'... van {i}0 minutos')

    log.info(f"{' FINALIZANDO PRUEBAS INICIADAS QUE COMENZARON A LAS {} ':▲^70}".format(hora_inicio))
    update_estado_finalizado(migracion.id)


if __name__ == '__main__':
    sched.start()
