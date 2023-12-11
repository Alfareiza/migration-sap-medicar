from base.models import RegistroMigracion


def crea_registro_migracion() -> RegistroMigracion:
    migracion = RegistroMigracion(estado='en ejecucion')
    migracion.save()
    return migracion


def update_estado(migracion: RegistroMigracion, estado: str) -> None:
    migracion.estado = estado
    migracion.save()


def update_estado_error(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'error')


def update_estado_finalizado(migracion_id: int) -> None:
    migracion = RegistroMigracion.objects.get(id=migracion_id)
    update_estado(migracion, 'finalizado')
