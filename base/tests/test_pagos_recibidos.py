import unittest

from base.tests.base_tests import TestsBaseAdvanced, CustomTestsMixin
from base.tests.conf_test import make_instance, pagos_recibidos_file
from utils.interactor_db import del_registro_migracion


class TestPagosRecibidos(CustomTestsMixin, unittest.TestCase):
    """python -m unittest base.tests.test_pagos_recibidos"""
    MODULE_NAME = 'pagos_recibidos'

    @classmethod
    def setUpClass(cls):
        fp = pagos_recibidos_file
        cls.module = make_instance(cls.MODULE_NAME, fp)
        cls.result = cls.module.exec_migration(tanda='TEST')

    @classmethod
    def tearDownClass(cls):
        del_registro_migracion(cls.module.migracion_id)

    def test_structrure(self):
        """Valida que vengan exactamente los keys esperados"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertCountEqual(
                    list(v['json'].keys()),
                    ["Series", "DocDate", "CardCode", "U_HBT_Tercero", "Remarks",
                     "JournalRemarks", "CashAccount", "CashSum", "ControlAccount"]
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertIsInstance(v['json']['Series'], int)
                self.assertIsInstance(v['json']['DocDate'], str)
                self.assertIsInstance(v['json']['CardCode'], str)
                self.assertIsInstance(v['json']['U_HBT_Tercero'], str)
                self.assertIsInstance(v['json']['Remarks'], str)
                self.assertIsInstance(v['json']['JournalRemarks'], str)
                self.assertIsInstance(v['json']['CashAccount'], str)
                self.assertIsInstance(v['json']['CashSum'], float)
                self.assertIsInstance(v['json']['ControlAccount'], str)

    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['Series'])
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['CardCode'])
                self.assertTrue(v['json']['U_HBT_Tercero'])
                self.assertTrue(v['json']['Remarks'])
                self.assertTrue(v['json']['JournalRemarks'])
                self.assertTrue(v['json']['CashAccount'] == '1105050101')
                self.assertTrue(v['json']['CashSum'])
                self.assertTrue(v['json']['ControlAccount'] == '2805950101')



class TestPagosRecibidosAdvanced(TestsBaseAdvanced):
    MODULE_NAME = 'pagos_recibidos'
    SOURCE_FILE = pagos_recibidos_file
