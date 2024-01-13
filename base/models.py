# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
from django.db.models import ForeignKey, CASCADE


class RegistroMigracion(models.Model):
    iniciado = models.DateTimeField(auto_now_add=True)
    finalizado = models.DateTimeField(auto_now=True, blank=True, null=True)
    estado = models.CharField(max_length=24)

    class Meta:
        db_table = 'sap_registro_migracion'

    def __str__(self):
        return f"{self.id} - {self.estado}"


class PayloadMigracion(models.Model):
    registrado = models.DateTimeField(auto_now_add=True)
    actualizado = models.DateTimeField(auto_now=True, blank=True, null=True)
    enviado_a_sap = models.BooleanField(default=False)
    status = models.TextField()
    migracion_id = ForeignKey(RegistroMigracion, blank=False, on_delete=CASCADE)
    modulo = models.CharField(max_length=64)

    ref_documento = models.CharField(max_length=32)
    valor_documento = models.CharField(max_length=32)

    nombre_archivo = models.CharField(max_length=128)
    cantidad_lineas_documento = models.IntegerField()
    payload = models.JSONField()
    lineas = models.TextField()

    class Meta:
        db_table = 'sap_payloads_en_migracion'
        unique_together = ('valor_documento', 'nombre_archivo')

    def __str__(self):
        return (f"<PayloadMigracionId:{self.id} {self.ref_documento}={self.valor_documento} "
                f"archivo={self.nombre_archivo} enviado_a_sap={self.enviado_a_sap}>")


class AuthGroup(models.Model):
    name = models.CharField(unique=True, max_length=150)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    name = models.CharField(max_length=255)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING)
    codename = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.BooleanField()
    username = models.CharField(unique=True, max_length=150)
    first_name = models.CharField(max_length=150)
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=254)
    is_staff = models.BooleanField()
    is_active = models.BooleanField()
    date_joined = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class BaseBarrio(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=128)
    zona = models.CharField(max_length=20)
    cod_zona = models.IntegerField()
    status = models.IntegerField()
    municipio = models.ForeignKey('BaseMunicipio', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'base_barrio'
        unique_together = (('municipio', 'name', 'zona', 'cod_zona'),)


class BaseCentros(models.Model):
    id = models.BigAutoField(primary_key=True)
    disp = models.CharField(max_length=24)
    bod = models.CharField(max_length=24)
    drogueria = models.CharField(max_length=128)
    correo_coordinador = models.CharField(max_length=128, blank=True, null=True)
    dia_ped = models.CharField(max_length=24, blank=True, null=True)
    estado = models.CharField(max_length=24, blank=True, null=True)
    modalidad = models.CharField(max_length=24)
    poblacion = models.IntegerField(blank=True, null=True)
    tipo = models.CharField(max_length=24)
    correo_disp = models.CharField(max_length=128, blank=True, null=True)
    responsable = models.CharField(max_length=128, blank=True, null=True)
    cedula = models.CharField(max_length=64, blank=True, null=True)
    celular = models.CharField(max_length=64, blank=True, null=True)
    direccion = models.CharField(max_length=128, blank=True, null=True)
    medicar = models.CharField(max_length=8, blank=True, null=True)
    tent = models.IntegerField()
    analista = models.CharField(max_length=128, blank=True, null=True)
    ult_fecha_disp = models.DateTimeField(blank=True, null=True)
    aux_pqr = models.CharField(max_length=128, blank=True, null=True)
    transp_1 = models.CharField(max_length=128, blank=True, null=True)
    transp_2 = models.CharField(max_length=128, blank=True, null=True)
    correo_contacto_eps = models.CharField(max_length=128, blank=True, null=True)
    municipio = models.ForeignKey('BaseMunicipio', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'base_centros'


class BaseInventario(models.Model):
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField()
    centro = models.CharField(max_length=24)
    cod_mol = models.CharField(max_length=24)
    cod_barra = models.CharField(max_length=128)
    cum = models.CharField(max_length=64, blank=True, null=True)
    descripcion = models.CharField(max_length=250)
    lote = models.CharField(max_length=24)
    fecha_vencimiento = models.DateField()
    inventario = models.IntegerField()
    costo_promedio = models.IntegerField()
    cantidad_empaque = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'base_inventario'


class BaseMedControlado(models.Model):
    id = models.BigAutoField(primary_key=True)
    cum = models.CharField(max_length=24)
    nombre = models.CharField(max_length=250)
    activo = models.BooleanField()
    field_one = models.CharField(max_length=24, blank=True, null=True)
    field_two = models.CharField(max_length=24, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'base_med_controlado'


class BaseMunicipio(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=128)
    departamento = models.CharField(max_length=128)
    activo = models.BooleanField()
    cod_dane = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'base_municipio'
        unique_together = (('name', 'departamento'),)


class BaseRadicacion(models.Model):
    id = models.BigAutoField(primary_key=True)
    datetime = models.DateTimeField()
    numero_radicado = models.CharField(unique=True, max_length=24)
    cel_uno = models.CharField(max_length=24, blank=True, null=True)
    cel_dos = models.CharField(max_length=24, blank=True, null=True)
    email = models.CharField(max_length=254)
    direccion = models.CharField(max_length=150)
    ip = models.GenericIPAddressField()
    paciente_nombre = models.CharField(max_length=150)
    paciente_cc = models.CharField(max_length=32)
    paciente_data = models.JSONField()
    domiciliario_nombre = models.CharField(max_length=150, blank=True, null=True)
    domiciliario_ide = models.CharField(max_length=25, blank=True, null=True)
    domiciliario_empresa = models.CharField(max_length=150, blank=True, null=True)
    estado = models.CharField(max_length=64, blank=True, null=True)
    alistamiento = models.DateTimeField(blank=True, null=True)
    alistado_por = models.CharField(max_length=150, blank=True, null=True)
    despachado = models.DateTimeField(blank=True, null=True)
    acta_entrega = models.CharField(max_length=150, blank=True, null=True)
    factura = models.CharField(max_length=150, blank=True, null=True)
    barrio = models.ForeignKey(BaseBarrio, models.DO_NOTHING)
    municipio = models.ForeignKey(BaseMunicipio, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'base_radicacion'


class DjangoAdminLog(models.Model):
    action_time = models.DateTimeField()
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.CharField(max_length=200)
    action_flag = models.SmallIntegerField()
    change_message = models.TextField()
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True, null=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    app_label = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    id = models.BigAutoField(primary_key=True)
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField()
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_session'


class FacturasProcesadas(models.Model):
    id = models.BigAutoField(primary_key=True)
    agregado = models.DateTimeField()
    actualizado = models.DateTimeField()
    factura = models.CharField(unique=True, max_length=24)
    fecha_factura = models.DateTimeField()
    acta = models.CharField(max_length=24)
    numero_autorizacion = models.CharField(unique=True, max_length=24)
    valor_total = models.IntegerField(blank=True, null=True)
    link_soporte = models.CharField(max_length=254, blank=True, null=True)
    estado = models.CharField(max_length=128, blank=True, null=True)
    resp_cajacopi = models.JSONField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'facturas_procesadas'
