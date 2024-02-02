import unittest

from base.tests.common_tests import CustomTestsMixin, DocumentLinesTestsMixin
from base.tests.conf_test import ajustes_salida_file, make_instance


class TestAjustesSalida(CustomTestsMixin, DocumentLinesTestsMixin, unittest.TestCase):
    MODULE_NAME = 'ajustes_salida'

    @classmethod
    def setUpClass(cls):
        fp = ajustes_salida_file
        cls.module = make_instance(cls.MODULE_NAME, fp)
        cls.result = cls.module.exec_migration(tanda='TEST')

    @classmethod
    def tearDownClass(cls):
        ...

    def test_structrure(self):
        """Valida que vengan exactamente los keys esperados"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertCountEqual(
                    list(v['json'].keys()),
                    ["Series", "DocDate", "DocDueDate", "Comments",
                     "U_HBT_Tercero", "DocumentLines"]
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(isinstance(v['json']['Series'], int))
                self.assertTrue(isinstance(v['json']['DocDate'], str))
                self.assertTrue(isinstance(v['json']['DocDueDate'], str))
                self.assertTrue(isinstance(v['json']['U_HBT_Tercero'], str))
                self.assertTrue(isinstance(v['json']['Comments'], str))
                self.assertTrue(isinstance(v['json']['DocumentLines'], list))
    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['Series'])
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['DocDueDate'])
                self.assertTrue(v['json']['U_HBT_Tercero'])
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
                            ["ItemCode", "WarehouseCode", "CostingCode",
                             "CostingCode2", "Quantity", "AccountCode",
                             "BatchNumbers"]
                        )

    def test_types_document_lines(self):
        """Valida los tipos de datos del documentlines."""
        for k, v in self.result.data.items():
            for document in v['json']['DocumentLines']:
                with self.subTest(i=document):
                    self.assertTrue(isinstance(document['ItemCode'], str))
                    self.assertTrue(isinstance(document['Quantity'], int))
                    self.assertTrue(isinstance(document['WarehouseCode'], str))
                    self.assertTrue(isinstance(document['AccountCode'], str))
                    self.assertTrue(isinstance(document['CostingCode'], str))
                    self.assertTrue(isinstance(document['CostingCode2'], str))
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
                        self.assertCountEqual(batch.keys(), ["BatchNumber", "Quantity"])

    def test_types_batch_numbers(self):
        """Valida los tipos de datos del BatchNumbers."""
        for k, v in self.result.data.items():
            for line in v['json']['DocumentLines']:
                for batch in line['BatchNumbers']:
                    with self.subTest(i=batch):
                        self.assertTrue(isinstance(batch['BatchNumber'], str))
                        self.assertTrue(isinstance(batch['Quantity'], int))

    def test_logic_batch_numbers(self):
        """
        Valida que la cantidad total sea igual a la suma de las cantidades
        de los lotes.
        """
        for k, v in self.result.data.items():
            for batch in v['json']['DocumentLines']:
                with self.subTest(i=batch):
                    cant = batch['Quantity']
                    cant_in_batchs = sum(b['Quantity'] for b in batch['BatchNumbers'])
                    self.assertEqual(cant, cant_in_batchs)