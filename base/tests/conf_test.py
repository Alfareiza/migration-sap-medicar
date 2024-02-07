from core.settings import BASE_DIR
from utils.interactor_db import crea_registro_migracion
from utils.parsers import Module
from utils.sap.connectors import SAPConnect
from utils.sap.manager import SAPData

MANAGER_SAP = SAPData()

compras_file = BASE_DIR / 'base/tests/samples/compras.csv'
traslados_file = BASE_DIR / 'base/tests/samples/traslados.csv'
ajustes_entrada_file = BASE_DIR / 'base/tests/samples/ajustes_entrada.csv'
ajustes_salida_file = BASE_DIR / 'base/tests/samples/ajustes_salida.csv'
ajustes_vencimiento_lote_file = BASE_DIR / 'base/tests/samples/ajustes_vencimiento_lote.csv'
dispensacion_file = BASE_DIR / 'base/tests/samples/dispensacion.csv'
dispensaciones_anuladas_file = BASE_DIR / 'base/tests/samples/dispensaciones_anuladas.csv'
facturacion_file = BASE_DIR / 'base/tests/samples/facturacion.csv'
notas_credito_file = BASE_DIR / 'base/tests/samples/notas_credito.csv'
pagos_recibidos_file = BASE_DIR / 'base/tests/samples/pagos_recibidos.csv'


def make_instance(module_name, filepath):
    mig = crea_registro_migracion(custom_status='running tests')
    return Module(name=module_name, filepath=filepath, sap=MANAGER_SAP, migracion_id=mig.id)


class ProcessFakeSAP(SAPConnect):

    def select_method(self):
        return self.fake_method
