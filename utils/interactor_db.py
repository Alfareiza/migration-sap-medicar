from typing import List
from base.models import RegistroMigracion, PayloadMigracion
from utils.converters import Csv2Dict
from utils.decorators import not_on_debug
from core.settings import logger as log


class DBHandler:

    def __init__(self, migracion_id, module_name, pk):
        self.mig = RegistroMigracion.objects.get(id=migracion_id)
        self.mname = module_name
        self.ref = pk
        self.fname = ''
        self.records = None  # QuerySet

    def process(self, csvtodict: Csv2Dict):
        log.info(f'[{self.mname}] guardando {len(csvtodict.data)} payloads en db')
        objs = self.create_objects(csvtodict)
        try:
            PayloadMigracion.objects.bulk_create(objs)
            self.records = PayloadMigracion.objects.filter(id__in=[p.id for p in objs])
        except Exception as e:
            log.error(f"Error {e} al guardar en db")
            raise
        else:
            log.info(f'[{self.mname}] {len(self.records)} payloads de archivo {self.mname} guardados en db')

    def create_objects(self, info: Csv2Dict) -> List[PayloadMigracion]:
        """Crea los PayloadMigracion con base en los payloads
        procesados previamente por ProcessCSV. """
        res = []
        for k in info.data:
            payload = PayloadMigracion(
                status=info.data[k]['csv'][0]['Status'],
                migracion_id=self.mig,
                modulo=self.mname,
                ref_documento=self.ref,
                valor_documento=k,
                nombre_archivo=self.fname,
                cantidad_lineas_documento=len(info.data[k]['csv']),
                payload=info.data[k]['json'],
                lineas=info.data[k]['csv'],
            )
            res.append(payload)
        return res


@not_on_debug
def crea_registro_migracion(custom_status='en ejecucion') -> RegistroMigracion:
    migracion = RegistroMigracion(estado=custom_status)
    migracion.save()
    return migracion


@not_on_debug
def update_estado(migracion: RegistroMigracion, estado: str) -> None:
    migracion.estado = estado
    migracion.save()


@not_on_debug
def update_estado_error(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'error')


@not_on_debug
def update_estado_error_drive(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'error drive')


@not_on_debug
def update_estado_error_sap(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'error sap')


@not_on_debug
def update_estado_error_export(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'error export')


@not_on_debug
def update_estado_error_mail(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'error mail')


@not_on_debug
def update_estado_error_heroku(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    if migracion.estado == 'en ejecucion':
        update_estado(migracion, 'heroku')


@not_on_debug
def update_estado_finalizado(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'finalizado')


def del_registro_migracion(migracion_id: int) -> None:
    log.debug(f'excluyendo migración #{migracion_id}')
    RegistroMigracion.objects.get(id=migracion_id).delete()
