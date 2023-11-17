import unittest

from base.tests.conf_test import dispensacion_file, facturacion_file, make_instance
from core.settings import BASE_DIR


class TestDispensacion(unittest.TestCase):
    """python -m unittest base.tests.test_dispensacion"""
    @classmethod
    def setUpClass(cls):
        fp = dispensacion_file
        cls.result = make_instance('dispensacion', fp)

    @classmethod
    def tearDownClass(cls):
        ...

    def is_capita(self, item):
        return item['json']['Series'] == 77

    def is_evento(self, item):
        return item['json']['Series'] == 81

    def test_structrure(self):
        """Valida que vengan exactamente los keys esperados"""
        for k, v in TestDispensacion.result.data.items():
            with self.subTest(i=v):
                if self.is_capita(v):
                    self.assertCountEqual(
                        list(v['json'].keys()),
                        ["Series", "DocDate", "U_HBT_Tercero", "Comments",
                         "U_LF_Plan", "U_LF_IdAfiliado", "U_LF_NombreAfiliado",
                         "U_LF_Formula", "U_LF_NivelAfiliado", "U_LF_Autorizacion",
                         "U_LF_Mipres", "U_LF_Usuario", "DocumentLines"]
                    )
                elif self.is_evento(v):
                    self.assertCountEqual(
                        list(v['json'].keys()),
                        ["Series", "DocDate", "TaxDate", "CardCode", "U_HBT_Tercero", "Comments",
                         "U_LF_Plan", "U_LF_IdAfiliado", "U_LF_NombreAfiliado",
                         "U_LF_Formula", "U_LF_NivelAfiliado", "U_LF_Autorizacion",
                         "U_LF_Mipres", "U_LF_Usuario", "DocumentLines"]
                    )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in TestDispensacion.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(isinstance(v['json']['Series'], int))
                self.assertTrue(isinstance(v['json']['DocDate'], str))
                self.assertTrue(isinstance(v['json']['U_HBT_Tercero'], str))
                self.assertTrue(isinstance(v['json']['Comments'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Plan'], str))
                self.assertTrue(isinstance(v['json']['U_LF_IdAfiliado'], str))
                self.assertTrue(isinstance(v['json']['U_LF_NombreAfiliado'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Formula'], str))
                self.assertTrue(isinstance(v['json']['U_LF_NivelAfiliado'], int))
                self.assertTrue(isinstance(v['json']['U_LF_Autorizacion'], int))
                self.assertTrue(isinstance(v['json']['U_LF_Mipres'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Usuario'], str))
                self.assertTrue(isinstance(v['json']['DocumentLines'], list))
                if self.is_evento(v):
                    self.assertTrue(isinstance(v['json']['CardCode'], str))
                    self.assertTrue(isinstance(v['json']['TaxDate'], str))
    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in TestDispensacion.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['Series'])
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['U_HBT_Tercero'])
                self.assertTrue(v['json']['Comments'])
                self.assertTrue(v['json']['U_LF_Plan'])
                self.assertTrue(v['json']['U_LF_IdAfiliado'])
                self.assertTrue(v['json']['U_LF_NombreAfiliado'])
                self.assertTrue(v['json']['U_LF_Formula'])
                self.assertTrue(v['json']['U_LF_NivelAfiliado'])
                self.assertTrue(v['json']['U_LF_Autorizacion'])
                # self.assertTrue(v['json']['U_LF_Mipres'])
                self.assertTrue(v['json']['U_LF_Usuario'])
                self.assertTrue(v['json']['DocumentLines'])
                if self.is_evento(v):
                    self.assertTrue(v['json']['CardCode'])
                    self.assertTrue(v['json']['TaxDate'])

    def test_structure_document_lines(self):
        """
        Valida que el documentlines tenga contenido y que sus keys sean
        los correctos.
        """
        for k, v in TestDispensacion.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(len(v['json']['DocumentLines']))
                for document in v['json']['DocumentLines']:
                    with self.subTest(i=document):
                        if self.is_capita(v):
                            self.assertCountEqual(
                                document.keys(),
                                ["ItemCode", "WarehouseCode", "CostingCode",
                                 "CostingCode2", "CostingCode3", "Quantity",
                                 "AccountCode", "BatchNumbers"]
                            )
                        if self.is_evento(v):
                            self.assertCountEqual(
                                document.keys(),
                                ["ItemCode", "WarehouseCode", "CostingCode",
                                 "CostingCode2", "CostingCode3", "Quantity",
                                 "BatchNumbers", "Price"]
                            )

    def test_types_document_lines(self):
        """Valida los tipos de datos del documentlines."""
        for k, v in TestDispensacion.result.data.items():
            for document in v['json']['DocumentLines']:
                with self.subTest(i=document):
                    self.assertTrue(isinstance(document['ItemCode'], str))
                    self.assertTrue(isinstance(document['Quantity'], int))
                    self.assertTrue(isinstance(document['WarehouseCode'], str))
                    self.assertTrue(isinstance(document['CostingCode'], str))
                    self.assertTrue(isinstance(document['CostingCode2'], str))
                    self.assertTrue(isinstance(document['CostingCode3'], str))
                    self.assertTrue(isinstance(document['BatchNumbers'], list))
                    if self.is_evento(v):
                        self.assertTrue(isinstance(document['Price'], float))
                    if self.is_capita(v):
                        self.assertTrue(isinstance(document['AccountCode'], str))

    def test_batch_numbers(self):
        """Valida que BatchNumbers tenga al menos un elemento
        y que cada uno sea de tipo diccionario."""
        for k, v in TestDispensacion.result.data.items():
            for line in v['json']['DocumentLines']:
                with self.subTest(i=line):
                    self.assertTrue(line['BatchNumbers'])
                    self.assertTrue(isinstance(line['BatchNumbers'][0], dict))

    def test_structure_batch_numbers(self):
        """Valida que los keys de los BatchNumbers sean los correctos."""
        for k, v in TestDispensacion.result.data.items():
            for line in v['json']['DocumentLines']:
                for batch in line['BatchNumbers']:
                    with self.subTest(i=batch):
                        self.assertCountEqual(batch.keys(), ["BatchNumber", "Quantity"])

    def test_types_batch_numbers(self):
        """Valida los tipos de datos del BatchNumbers."""
        for k, v in TestDispensacion.result.data.items():
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
        for k, v in TestDispensacion.result.data.items():
            for batch in v['json']['DocumentLines']:
                with self.subTest(i=batch):
                    cant = batch['Quantity']
                    cant_in_batchs = sum(b['Quantity'] for b in batch['BatchNumbers'])
                    self.assertEqual(cant, cant_in_batchs)
