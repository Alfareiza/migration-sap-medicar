import unittest

from base.tests.base_tests import TestsBaseAdvanced, DocumentLinesTestsMixin, CustomTestsMixin
from base.tests.conf_test import make_instance, ajustes_vencimiento_lote_file
from utils.interactor_db import del_registro_migracion


class TestAjustesVencimientoLote(CustomTestsMixin, unittest.TestCase):
    """python -m unittest base.tests.test_ajustes_vencimiento_lote"""
    MODULE_NAME = 'ajustes_vencimiento_lote'

    @classmethod
    def setUpClass(cls):
        fp = ajustes_vencimiento_lote_file
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
                    ["Series", "ExpirationDate"]
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                if k in self.result.errs:
                    self.assertIsNone(v['json']['Series'])
                else:
                    self.assertIsInstance(v['json']['Series'], int)
                self.assertIsInstance(v['json']['ExpirationDate'], str)

    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                if k in self.result.errs:
                    self.assertIsNone(v['json']['Series'])
                else:
                    self.assertIsInstance(v['json']['Series'], int)
                self.assertTrue(v['json']['ExpirationDate'])



class TestAjustesVencimientoLoteAdvanced(TestsBaseAdvanced):
    MODULE_NAME = 'ajustes_vencimiento_lote'
    SOURCE_FILE = ajustes_vencimiento_lote_file

    def test_payload_in_db(self):
        """ Valida que el json generado sea el mismo que está en el campo payload en la bd"""
        docs_in_db = self.docs_in_db('valor_documento', 'payload')

        for k, v in self.result.data.items():
            doc = docs_in_db.get(valor_documento=k)
            with self.subTest(i=v):
                del doc.payload['Series']
                self.assertEqual(doc.payload, v['json'])
