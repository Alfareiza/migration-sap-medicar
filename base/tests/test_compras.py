import unittest

from base.tests.base_tests import TestsBaseAdvanced, DocumentLinesTestsMixin, CustomTestsMixin
from base.tests.conf_test import make_instance, compras_file
from utils.interactor_db import del_registro_migracion


class TestCompras(CustomTestsMixin, DocumentLinesTestsMixin, unittest.TestCase):
    """python -m unittest base.tests.test_compras"""
    MODULE_NAME = 'compras'

    @classmethod
    def setUpClass(cls):
        fp = compras_file
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
                    ["Series", "DocDate", "NumAtCard", "CardCode",
                     "U_LF_NroDocumento", "Comments", "DocumentLines"]
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(isinstance(v['json']['Series'], int))
                self.assertTrue(isinstance(v['json']['U_LF_NroDocumento'], str))
                self.assertTrue(isinstance(v['json']['DocDate'], str))
                self.assertTrue(isinstance(v['json']['NumAtCard'], str))
                self.assertTrue(isinstance(v['json']['CardCode'], str))
                self.assertTrue(isinstance(v['json']['Comments'], str))
                self.assertTrue(isinstance(v['json']['DocumentLines'], list))

    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['Series'])
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['U_LF_NroDocumento'].startswith('Comp'))
                self.assertTrue(v['json']['CardCode'])
                self.assertTrue(v['json']['NumAtCard'])
                self.assertTrue(v['json']['Comments'])
                self.assertTrue(v['json']['DocumentLines'])

    def test_structure_document_lines(self):
        """
        Valida que el documentlines tenga contenido y que sus keys sean
        los correctos.
        """
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(len(v['json']['DocumentLines']))
                for document in v['json']['DocumentLines']:
                    with self.subTest(i=document):
                        self.assertCountEqual(
                            document.keys(),
                            ["ItemCode", "Quantity", "WarehouseCode",
                             "UnitPrice", "BatchNumbers"]
                        )

    def test_types_document_lines(self):
        """Valida los tipos de datos del documentlines."""
        for k, v in self.result.data.items():
            for document in v['json']['DocumentLines']:
                with (self.subTest(i=document)):
                    self.assertTrue(isinstance(document['ItemCode'], str))
                    if document['Quantity']: # puede ser None
                        self.assertTrue(isinstance(document['Quantity'], int))
                    self.assertTrue(isinstance(document['WarehouseCode'], str))
                    self.assertTrue(isinstance(document['UnitPrice'], float))
                    self.assertTrue(isinstance(document['BatchNumbers'], list))

    def test_batch_numbers(self):
        """Valida que BatchNumbers tenga al menos un elemento
        y que cada uno sea de tipo diccionario."""
        for k, v in self.result.data.items():
            for line in v['json']['DocumentLines']:
                with self.subTest(i=line):
                    self.assertTrue(line['BatchNumbers'])
                    self.assertTrue(isinstance(line['BatchNumbers'][0], dict))

    def test_structure_batch_numbers(self):
        """Valida que los keys de los BatchNumbers sean los correctos."""
        for k, v in self.result.data.items():
            for line in v['json']['DocumentLines']:
                for batch in line['BatchNumbers']:
                    with self.subTest(i=batch):
                        self.assertCountEqual(
                            batch.keys(),
                            ["BatchNumber", "Quantity", "ExpiryDate"]
                        )

    def test_types_batch_numbers(self):
        """Valida los tipos de datos del BatchNumbers."""
        for k, v in self.result.data.items():
            for line in v['json']['DocumentLines']:
                for batch in line['BatchNumbers']:
                    with self.subTest(i=batch):
                        self.assertTrue(isinstance(batch['BatchNumber'], str))
                        self.assertTrue(isinstance(batch['Quantity'], int))
                        self.assertTrue(isinstance(batch['ExpiryDate'], str))


class TestComprasAdvanced(TestsBaseAdvanced):
    MODULE_NAME = 'compras'
    SOURCE_FILE = compras_file
