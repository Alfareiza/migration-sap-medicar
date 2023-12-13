from base.models import RegistroMigracion
from utils.decorators import not_on_debug


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
def update_estado_finalizado(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'finalizado')
