from core.settings import BASE_DIR
from utils.parsers import Module
from utils.sap.manager import SAPData

MANAGER_SAP = SAPData()

dispensacion_file = BASE_DIR / 'base/tests/samples/dispensacion.csv'
facturacion_file = BASE_DIR / 'base/tests/samples/facturacion.csv'
traslados_file = BASE_DIR / 'base/tests/samples/traslados.csv'
ajustes_entrada_file = BASE_DIR / 'base/tests/samples/ajustes_entrada.csv'
ajustes_salida_file = BASE_DIR / 'base/tests/samples/ajustes_salida.csv'
notas_credito_file = BASE_DIR / 'base/tests/samples/notas_credito.csv'


def make_instance(module_name, filepath):
    module = Module(name=module_name, filepath=filepath, sap=MANAGER_SAP)
    return module.exec_migration(export=False)
