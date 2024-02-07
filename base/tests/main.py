import importlib
import inspect
import re
import unittest

import os;

os.environ['DJANGO_SETTINGS_MODULE'] = 'core.settings';
import django;

django.setup();

MODULES_TO_TEST = (
    'test_compras',
    'test_traslados',
    'test_ajustes_entrada',
    'test_ajustes_salida',
    'test_ajustes_vencimiento_lote',
    'test_dispensacion',
    'test_dispensaciones_anuladas',
    'test_facturacion',
    'test_notas_credito',
    'test_pagos_recibidos',
)


def get_classes_to_test() -> list:
    classes_to_be_tested = []
    for module_name in MODULES_TO_TEST:
        module_imported = importlib.import_module(module_name)
        classes_detected = inspect.getmembers(module_imported, inspect.isclass)
        for name_class, class_detected in classes_detected:
            if module_name in class_detected.__module__:
                classes_to_be_tested.append(class_detected)

    return classes_to_be_tested


def suite():
    suite = unittest.TestSuite()
    class_to_test = get_classes_to_test()
    pattern = r'^test_.*$'
    for cla in class_to_test:
        funcs = dir(cla)
        for func in funcs:
            if match := re.match(pattern, func):
                name_func = match[0]
                suite.addTest(cla(name_func))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
