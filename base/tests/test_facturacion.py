import unittest

from base.tests.conf_test import facturacion_file, make_instance
from base.tests.base_tests import DocumentLinesTestsMixin, CustomTestsMixin, TestsBaseAdvanced
from utils.interactor_db import del_registro_migracion


# Escenario 5.1
class TestFacturacion(DocumentLinesTestsMixin, CustomTestsMixin, unittest.TestCase):
    MODULE_NAME = 'facturacion'

    @classmethod
    def setUpClass(cls):
        fp = facturacion_file
        cls.module = make_instance(cls.MODULE_NAME, fp)
        cls.result = cls.module.exec_migration(tanda='TEST')

    @classmethod
    def tearDownClass(cls):
        del_registro_migracion(cls.module.migracion_id)

    def is_391(self, document):
        return document.get('WarehouseCode', '') == '391'

    def test_structrure(self):
        """Valida que vengan exactamente los keys esperados"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertEqual(
                    list(v['json'].keys()),
                    ["Comments", "U_LF_IdAfiliado", "U_LF_Formula",
                     "U_LF_Mipres", "U_LF_Usuario", "Series", "DocDate",
                     "TaxDate", "NumAtCard", "CardCode", "U_HBT_Tercero",
                     "U_LF_Plan", "U_LF_NivelAfiliado", "U_LF_NombreAfiliado",
                     "U_LF_Autorizacion", "DocumentLines",
                     "WithholdingTaxDataCollection"],
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertIsInstance(v['json']['Series'], int)
                self.assertIsInstance(v['json']['CardCode'], str)
                self.assertIsInstance(v['json']['DocDate'], str)
                self.assertIsInstance(v['json']['TaxDate'], str)
                self.assertIsInstance(v['json']['NumAtCard'], str)
                self.assertIsInstance(v['json']['U_HBT_Tercero'], str)
                self.assertIsInstance(v['json']['Comments'], str)
                self.assertIsInstance(v['json']['U_LF_IdAfiliado'], str)
                self.assertIsInstance(v['json']['U_LF_Formula'], str)
                self.assertIsInstance(v['json']['U_LF_Mipres'], str)
                self.assertIsInstance(v['json']['U_LF_Usuario'], str)
                self.assertIsInstance(v['json']['U_LF_Plan'], str)
                self.assertIsInstance(v['json']['U_LF_NivelAfiliado'], int)
                self.assertIsInstance(v['json']['U_LF_NombreAfiliado'], str)
                self.assertIsInstance(v['json']['U_LF_Autorizacion'], int)
                self.assertIsInstance(v['json']['DocumentLines'], list)
                self.assertIsInstance(v['json']['WithholdingTaxDataCollection'], list)

    def test_content_in_structrure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['Series'])
                self.assertTrue(v['json']['CardCode'])
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['TaxDate'])
                self.assertTrue(v['json']['NumAtCard'])
                self.assertTrue(v['json']['U_HBT_Tercero'])
                self.assertTrue(v['json']['Comments'])
                self.assertTrue(v['json']['U_LF_IdAfiliado'])
                self.assertTrue(v['json']['U_LF_Formula'])
                # self.assertTrue(v['json']['U_LF_Mipres'])
                self.assertTrue(v['json']['U_LF_Usuario'])
                self.assertTrue(v['json']['U_LF_Plan'])
                self.assertTrue(v['json']['U_LF_NivelAfiliado'])
                self.assertTrue(v['json']['U_LF_NombreAfiliado'])
                self.assertTrue(v['json']['U_LF_Autorizacion'])
                self.assertTrue(v['json']['DocumentLines'])
                self.assertTrue(v['json']['WithholdingTaxDataCollection'])

    def test_structure_document_lines(self):
        """
        Valida que el documentlines tenga contenido y que sus keys sean
        los correctos.
        """
        for _, v in self.result.data.items():
            for document in v['json']['DocumentLines']:
                with self.subTest(i=document):
                    if self.is_391(document):
                        self.assertEqual(
                            sorted(list(document.keys())),
                            sorted(["ItemCode", "ItemDescription", "WarehouseCode",
                                    "CostingCode", "CostingCode2", "CostingCode3",
                                    "Quantity", "Price"])
                        )
                        self.assertRegex(document['ItemDescription'], r'^\d+\s(.+)$')
                    else:
                        self.assertEqual(
                            sorted(list(document.keys())),
                            sorted(["ItemCode", "WarehouseCode", "CostingCode",
                                    "CostingCode2", "CostingCode3", "Quantity",
                                    "Price", "BaseType", "BaseEntry", "BaseLine"])
                        )

    def test_types_document_lines(self):
        """Valida los tipos de datos del documentlines."""
        for k, v in self.result.data.items():
            for document in v['json']['DocumentLines']:
                with self.subTest(i=document):
                    self.assertIsInstance(document['ItemCode'], str)
                    self.assertIsInstance(document['Quantity'], int)
                    self.assertIsInstance(document['Price'], float)
                    if not self.is_391(document) and document.get('BaseType'):
                        self.assertIsInstance(document['BaseType'], str)
                    if not self.is_391(document) and document.get('BaseEntry'):
                        self.assertIsInstance(document['BaseEntry'], str)
                    if not self.is_391(document) and document['BaseLine']:
                        self.assertIsInstance(document['BaseLine'], int)
                    self.assertIsInstance(document['WarehouseCode'], str)
                    self.assertIsInstance(document['CostingCode'], str)
                    self.assertIsInstance(document['CostingCode2'], str)
                    self.assertIsInstance(document['CostingCode3'], str)
                    if self.is_391(document):
                        self.assertIsInstance(document['ItemDescription'], str)

    def test_structure_withholding_tax_data_collection(self):
        """Valida WithholdingTaxDataCollection."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                self.assertEqual(
                    sorted(list(v['json']['WithholdingTaxDataCollection'][0].keys())),
                    sorted(['Rate', 'U_HBT_Retencion', 'WTCode'])
                )

    def test_consistency_u_hbt_retencion(self):
        for k, v in self.result.data.items():
            subtotal = sum(art['Quantity'] * art['Price'] for art in v['json']['DocumentLines'])
            with self.subTest(i=v):
                self.assertEqual(
                    v['json']['WithholdingTaxDataCollection'][0]['U_HBT_Retencion'],
                    subtotal
                )


class TestNotasCreditoAdvanced(TestsBaseAdvanced):
    MODULE_NAME = 'facturacion'
    SOURCE_FILE = facturacion_file
