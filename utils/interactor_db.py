from typing import List

from base.models import RegistroMigracion, PayloadMigracion
from utils.converters import Csv2Dict
from utils.decorators import not_on_debug
from core.settings import logger as log


class DBHandler:
    def __init__(self, migracion_id, filename, module_name, pk):
        self.mig = RegistroMigracion.objects.get(id=migracion_id)
        self.fname = filename
        self.mname = module_name
        self.ref = pk
        self.registros = []

    def process(self, csvtodict: Csv2Dict):
        log.info(f'[{self.mname}] guardando payloads en db')
        self.registros = self.create_objects(csvtodict)
        PayloadMigracion.objects.bulk_create(self.registros)
        log.info(f'[{self.mname}] {len(self.registros)} payloads de archivo {self.mname} guardados en db')

    def create_objects(self, info: Csv2Dict) -> List[PayloadMigracion]:
        """ Crea los PayloadMigracion con base en los payloads
        procesados previamente por ProcessCSV. """
        res = []
        for k in info.succss:
            payload = PayloadMigracion(migracion_id=self.mig,
                                       nombre_archivo=self.fname,
                                       documento_referencia=self.ref,
                                       valor_referencia=k,
                                       modulo=self.mname,
                                       cantidad_lineas=len(info.data[k]['csv']),
                                       lineas=info.data[k]['csv'],
                                       payload=info.data[k]['json'])
            res.append(payload)
        return res


@not_on_debug
def crea_registro_migracion() -> RegistroMigracion:
    migracion = RegistroMigracion(estado='en ejecucion')
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
def update_estado_finalizado(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'finalizado')
