import unittest

from base.tests.conf_test import traslados_file, make_instance
from utils.interactor_db import del_registro_migracion


class TestTraslados(unittest.TestCase):
    MODULE_NAME = 'traslados'

    @classmethod
    def setUpClass(cls):
        fp = traslados_file
        cls.module = make_instance(cls.MODULE_NAME, fp)
        cls.result = cls.module.exec_migration(tanda='TEST')

    @classmethod
    def tearDownClass(cls):
        del_registro_migracion(cls.module.migracion_id)

    def test_structrure(self):
        """Valida que vengan exactamente los keys esperados"""
        for k, v in TestTraslados.result.data.items():
            with self.subTest(i=v):
                self.assertCountEqual(
                    v['json'].keys(),
                    ["JournalMemo", "DocDate", "CardCode",
                     "FromWarehouse", "ToWarehouse", "StockTransferLines"],
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in TestTraslados.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(isinstance(v['json']['DocDate'], str))
                self.assertTrue(isinstance(v['json']['CardCode'], str))
                self.assertTrue(isinstance(v['json']['JournalMemo'], str))
                self.assertTrue(isinstance(v['json']['FromWarehouse'], str))
                self.assertTrue(isinstance(v['json']['ToWarehouse'], str))
                self.assertTrue(isinstance(v['json']['StockTransferLines'], list))

    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in TestTraslados.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['CardCode'])
                self.assertTrue(v['json']['JournalMemo'])
                self.assertTrue(v['json']['FromWarehouse'])
                self.assertTrue(v['json']['ToWarehouse'])
                self.assertTrue(v['json']['StockTransferLines'])

    def test_structure_stock_transfer_lines(self):
        """
        Valida que el StockTransferLines tenga contenido y que sus keys sean
        los correctos.
        """
        for k, v in TestTraslados.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(len(v['json']['StockTransferLines']))
                for document in v['json']['StockTransferLines']:
                    with self.subTest(i=document):
                        self.assertCountEqual(
                            document.keys(),
                            ["ItemCode", "LineNum", "BatchNumbers",
                             "Quantity", "StockTransferLinesBinAllocations"]
                        )

    def test_types_stock_transfer_lines(self):
        """Valida los tipos de datos del StockTransferLines."""
        for k, v in TestTraslados.result.data.items():
            for document in v['json']['StockTransferLines']:
                with self.subTest(i=document):
                    self.assertTrue(isinstance(document['ItemCode'], str))
                    self.assertTrue(isinstance(document['Quantity'], int))
                    self.assertTrue(isinstance(document['LineNum'], int))
                    self.assertTrue(isinstance(document['BatchNumbers'], list))
                    self.assertTrue(isinstance(document['StockTransferLinesBinAllocations'], list))

    def test_batch_numbers(self):
        """
        Valida que BatchNumbers tenga al menos un elemento
        y que cada uno sea de tipo diccionario"""
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                with self.subTest(i=line):
                    self.assertTrue(line['BatchNumbers'])
                    self.assertTrue(isinstance(line['BatchNumbers'][0], dict))

    def test_structure_batch_numbers(self):
        """
        Valida que los keys de los BatchNumbers sean los correctos.
        """
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                for batch in line['BatchNumbers']:
                    with self.subTest(i=batch):
                        self.assertCountEqual(batch.keys(), ["BatchNumber", "Quantity"])

    def test_types_batch_numbers(self):
        """Valida los tipos de datos del BatchNumbers"""
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                for batch in line['BatchNumbers']:
                    with self.subTest(i=batch):
                        self.assertTrue(isinstance(batch['BatchNumber'], str))
                        self.assertTrue(isinstance(batch['Quantity'], int))

    def test_logic_batch_numbers(self):
        """Valida que la cantidad total sea igual a la suma de las cantidades
        de los lotes."""
        for k, v in TestTraslados.result.data.items():
            for batch in v['json']['StockTransferLines']:
                with self.subTest(i=batch):
                    cant = batch['Quantity']
                    cant_in_batchs = sum(b['Quantity'] for b in batch['BatchNumbers'])
                    self.assertEqual(cant, cant_in_batchs)

    def test_stock_transfer_lines_bin_allocations(self):
        """Valida que StockTransferLinesBinAllocations tenga dos elementos
        y que cada uno sea de tipo diccionário"""
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                with self.subTest(i=line):
                    self.assertTrue(len(line['StockTransferLinesBinAllocations']) == 2)
                    self.assertTrue(isinstance(line['StockTransferLinesBinAllocations'][0], dict))
                    self.assertTrue(isinstance(line['StockTransferLinesBinAllocations'][1], dict))

    def test_structure_stock_transfer_lines_bin_allocations(self):
        """Valida los tipos de datos del StockTransferLines."""
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                for bin in line['StockTransferLinesBinAllocations']:
                    with self.subTest(i=bin):
                        self.assertCountEqual(
                            bin.keys(),
                            ["BinAbsEntry", "Quantity", "BaseLineNumber",
                             "BinActionType", "SerialAndBatchNumbersBaseLine"]
                        )

    def test_types_stock_transfer_lines_bin_allocations(self):
        """Valida los tipos de datos del StockTransferLines."""
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                for bin in line['StockTransferLinesBinAllocations']:
                    with self.subTest(i=bin):
                        self.assertTrue(isinstance(bin['BinAbsEntry'], int))
                        self.assertTrue(isinstance(bin['Quantity'], int))
                        self.assertTrue(isinstance(bin['BaseLineNumber'], int))
                        self.assertTrue(isinstance(bin['BinActionType'], str))
                        self.assertTrue(isinstance(bin['SerialAndBatchNumbersBaseLine'], int))

    def test_logic_stock_transfer_lines_bin_allocations(self):
        """Valida que los dos elementos de StockTransferLinesBinAllocations
        tengan to do igual excepto el BinAbsEntry"""
        for k, v in TestTraslados.result.data.items():
            for line in v['json']['StockTransferLines']:
                with self.subTest(i=line):
                    bin_one = line['StockTransferLinesBinAllocations'][0]
                    bin_two = line['StockTransferLinesBinAllocations'][1]
                    self.assertNotEqual(bin_one['BinAbsEntry'], bin_two['BinAbsEntry'])
                    self.assertEqual(bin_one['Quantity'], bin_two['Quantity'])
                    self.assertEqual(bin_one['BaseLineNumber'], bin_two['BaseLineNumber'])
                    self.assertEqual(bin_one['BinActionType'], 'batFromWarehouse')
                    self.assertEqual(bin_two['BinActionType'], 'batToWarehouse')
                    self.assertEqual(bin_one['SerialAndBatchNumbersBaseLine'], bin_two['SerialAndBatchNumbersBaseLine'])
