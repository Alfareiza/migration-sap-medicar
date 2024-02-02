import unittest

from base.tests.common_tests import DocumentLinesTestsMixin, CustomTestsMixin
from base.tests.conf_test import notas_credito_file, make_instance
from utils.interactor_db import del_registro_migracion


class TestNotasCredito(DocumentLinesTestsMixin, CustomTestsMixin, unittest.TestCase):
    MODULE_NAME = 'notas_credito'
    @classmethod
    def setUpClass(cls):
        fp = notas_credito_file
        cls.module = make_instance(cls.MODULE_NAME, fp)
        cls.result = cls.module.exec_migration(tanda='TEST')

    @classmethod
    def tearDownClass(cls):
        del_registro_migracion(cls.module.migracion_id)

    def test_structrure(self):
        """Valida que vengan exactamente los keys esperados"""
        for k, v in TestNotasCredito.result.data.items():
            with self.subTest(i=v):
                self.assertEqual(
                    sorted(list(v['json'].keys())),
                    sorted(["Comments", "U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario", "Series", "DocDate",
                            "TaxDate", "NumAtCard", "CardCode", "U_HBT_Tercero",
                            "U_LF_Plan", "U_LF_NivelAfiliado", "U_LF_NombreAfiliado",
                            "U_LF_Autorizacion", "DocumentLines"]),
                )

    def test_types_in_structrure(self):
        """Valida los tipos de cada value"""
        for k, v in TestNotasCredito.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(isinstance(v['json']['Series'], int))
                self.assertTrue(isinstance(v['json']['CardCode'], str))
                self.assertTrue(isinstance(v['json']['DocDate'], str))
                self.assertTrue(isinstance(v['json']['TaxDate'], str))
                self.assertTrue(isinstance(v['json']['NumAtCard'], str))
                self.assertTrue(isinstance(v['json']['U_HBT_Tercero'], str))
                self.assertTrue(isinstance(v['json']['Comments'], str))
                self.assertTrue(isinstance(v['json']['U_LF_IdAfiliado'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Formula'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Mipres'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Usuario'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Plan'], str))
                self.assertTrue(isinstance(v['json']['U_LF_NivelAfiliado'], int))
                self.assertTrue(isinstance(v['json']['U_LF_NombreAfiliado'], str))
                self.assertTrue(isinstance(v['json']['U_LF_Autorizacion'], int))
                self.assertTrue(isinstance(v['json']['DocumentLines'], list))

    def test_content_in_structure(self):
        """Valida que hayan datos en los keys obligatórios."""
        for k, v in TestNotasCredito.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(v['json']['Series'])
                self.assertTrue(v['json']['CardCode'])
                self.assertTrue(v['json']['DocDate'])
                self.assertTrue(v['json']['TaxDate'])
                self.assertTrue(v['json']['NumAtCard'])
                self.assertTrue(v['json']['U_HBT_Tercero'])
                # self.assertTrue(v['json']['Comments'])
                self.assertTrue(v['json']['U_LF_IdAfiliado'])
                self.assertTrue(v['json']['U_LF_Formula'])
                # self.assertTrue(v['json']['U_LF_Mipres'])
                self.assertTrue(v['json']['U_LF_Usuario'])
                self.assertTrue(v['json']['U_LF_Plan'])
                self.assertTrue(v['json']['U_LF_NivelAfiliado'])
                self.assertTrue(v['json']['U_LF_NombreAfiliado'])
                self.assertTrue(v['json']['U_LF_Autorizacion'])
                self.assertTrue(v['json']['DocumentLines'])

    def test_structure_document_lines(self):
        """ Valida que el documentlines tenga contenido y que sus keys sean los correctos. """
        for k, v in TestNotasCredito.result.data.items():
            with self.subTest(i=v):
                self.assertTrue(len(v['json']['DocumentLines']))
                for document in v['json']['DocumentLines']:
                    with self.subTest(i=document):
                        self.assertCountEqual(
                            document.keys(),
                            ["ItemCode", "WarehouseCode", "CostingCode",
                             "CostingCode2", "CostingCode3", "Quantity",
                             "Price", "BaseType", "BaseEntry", "BaseLine",
                             "BatchNumbers", "StockInmPrice"]
                        )

    def test_types_document_lines(self):
        """Valida los tipos de datos del documentlines."""
        for k, v in TestNotasCredito.result.data.items():
            for document in v['json']['DocumentLines']:
                with self.subTest(i=document):
                    self.assertTrue(isinstance(document['ItemCode'], str))
                    self.assertTrue(isinstance(document['Quantity'], int))
                    self.assertTrue(isinstance(document['Price'], float))
                    if document['StockInmPrice']:
                        self.assertTrue(isinstance(document['StockInmPrice'], float))
                    self.assertTrue(isinstance(document['BaseType'], str))
                    if document['BaseEntry']:
                        self.assertTrue(isinstance(document['BaseEntry'], int))
                    if document['BaseLine']:
                        self.assertTrue(isinstance(document['BaseLine'], int))
                    self.assertTrue(isinstance(document['WarehouseCode'], str))
                    self.assertTrue(isinstance(document['CostingCode'], str))
                    self.assertTrue(isinstance(document['CostingCode2'], str))
                    self.assertTrue(isinstance(document['CostingCode3'], str))
                    self.assertTrue(isinstance(document['BatchNumbers'], list))
