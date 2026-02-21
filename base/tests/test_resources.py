import unittest
from unittest.mock import Mock

from utils.converters import Csv2Dict
from utils.resources import build_new_documentlines


class TestGetCentroDeCosto(unittest.TestCase):
    def setUp(self):
        # Create a mock for the SAPData dependency
        self.mock_sap = Mock()
        # Instantiate Csv2Dict
        self.converter = Csv2Dict(
            name='test_module',
            pk='id',
            series={'CAPITA': 89, 'EVENTO': 11},
            sap=self.mock_sap
        )

    def test_get_centro_de_costo_capita_group(self):
        test_cases = [
            "CAPITA", "CAPITA SUBSIDIADO", "CAPITA NUEVA EPS DISFARMA",
            "CAPITA COMPLEMENTARIA SUBSIDIADO", "CAPITA BASICA SUBSIDIADO"
        ]
        expected = "7165950102"
        for sub_plan in test_cases:
            with self.subTest(sub_plan=sub_plan):
                row = {'id': '1', 'SubPlan': sub_plan}
                result = self.converter.get_centro_de_costo(row, 'SubPlan')
                self.assertEqual(result, expected)

    def test_get_centro_de_costo_contributivo_group(self):
        test_cases = ["CAPITA CONTRIBUTIVO", "CAPITA COMPLEMENTARIA CONTRIBUTIVO"]
        expected = "7165950101"
        for sub_plan in test_cases:
            with self.subTest(sub_plan=sub_plan):
                row = {'id': '1', 'SubPlan': sub_plan}
                result = self.converter.get_centro_de_costo(row, 'SubPlan')
                self.assertEqual(result, expected)

    def test_get_centro_de_costo_evento_pbs_contributivo_group(self):
        test_cases = ["EVENTO PBS CONTRIBUTIVO", "CAPITA BASICA CONTRIBUTIVO"]
        expected = "7165950202"
        for sub_plan in test_cases:
            with self.subTest(sub_plan=sub_plan):
                row = {'id': '1', 'SubPlan': sub_plan}
                result = self.converter.get_centro_de_costo(row, 'SubPlan')
                self.assertEqual(result, expected)

    def test_get_centro_de_costo_evento_no_pbs_subsidiado(self):
        row = {'id': '1', 'SubPlan': "EVENTO NO PBS SUBSIDIADO"}
        result = self.converter.get_centro_de_costo(row, 'SubPlan')
        self.assertEqual(result, "7165950203")

    def test_get_centro_de_costo_evento_no_pbs_contributivo(self):
        row = {'id': '1', 'SubPlan': "EVENTO NO PBS CONTRIBUTIVO"}
        result = self.converter.get_centro_de_costo(row, 'SubPlan')
        self.assertEqual(result, "7165950204")

    def test_get_centro_de_costo_magisterio_group(self):
        test_cases = ["MAGISTERIO MEDIFARMA EVENTO", "MAGISTERIO RAMEDICAS CAPITA", "MAGISTERIO FARMAT EVENTO"]
        expected = "7165950401"
        for sub_plan in test_cases:
            with self.subTest(sub_plan=sub_plan):
                row = {'id': '1', 'SubPlan': sub_plan}
                result = self.converter.get_centro_de_costo(row, 'SubPlan')
                self.assertEqual(result, expected)

    def test_get_centro_de_costo_evento_pbs_subsidiado(self):
        row = {'id': '1', 'SubPlan': "EVENTO PBS SUBSIDIADO"}
        result = self.converter.get_centro_de_costo(row, 'SubPlan')
        self.assertEqual(result, "7165950201")

    def test_get_centro_de_costo_ajuste_faltante(self):
        row = {'id': '1', 'TipoAjuste': "AJUSTE POR FALTANTE"}
        result = self.converter.get_centro_de_costo(row, 'TipoAjuste')
        self.assertEqual(result, "7165950301")

    def test_get_centro_de_costo_ajuste_sobrante(self):
        row = {'id': '1', 'TipoAjuste': "AJUSTE POR SOBRANTE"}
        result = self.converter.get_centro_de_costo(row, 'TipoAjuste')
        self.assertEqual(result, "7165950302")

    def test_get_centro_de_costo_ajuste_inventario_general(self):
        with self.subTest(tipo_ajuste='salida'):
            row = {'id': '1', 'TipoAjuste': "AJUSTE EN INVENTARIO GENERAL"}
            result = self.converter.get_centro_de_costo(row, 'TipoAjuste', tipo_ajuste='salida')
            self.assertEqual(result, "7165950301")
        with self.subTest(tipo_ajuste='entrada'):
            row = {'id': '1', 'TipoAjuste': "AJUSTE EN INVENTARIO GENERAL"}
            result = self.converter.get_centro_de_costo(row, 'TipoAjuste', tipo_ajuste='entrada')
            self.assertEqual(result, "7165950302")

    def test_get_centro_de_costo_averias(self):
        row = {'id': '1', 'TipoAjuste': "AVERIAS"}
        result = self.converter.get_centro_de_costo(row, 'TipoAjuste')
        self.assertEqual(result, "5310350102")

    def test_get_centro_de_costo_donacion_group(self):
        test_cases = ["SALIDA POR DONACION", "ENTRADA POR DONACION"]
        expected = "7165950303"
        for tipo_ajuste in test_cases:
            with self.subTest(tipo_ajuste=tipo_ajuste):
                row = {'id': '1', 'TipoAjuste': tipo_ajuste}
                result = self.converter.get_centro_de_costo(row, 'TipoAjuste')
                self.assertEqual(result, expected)

    def test_get_centro_de_costo_vencidos(self):
        row = {'id': '1', 'TipoAjuste': "VENCIDOS"}
        result = self.converter.get_centro_de_costo(row, 'TipoAjuste')
        self.assertEqual(result, "5310350102")

    def test_get_centro_de_costo_empty_string(self):
        row = {'id': '1', 'SubPlan': ""}
        result = self.converter.get_centro_de_costo(row, 'SubPlan')
        self.assertEqual(result, "")

    def test_get_centro_de_costo_unrecognized(self):
        row = {'id': '1', 'SubPlan': "UNRECOGNIZED_VALUE", 'Status': ''}
        key = row['id']
        self.converter.succss.add(key)
        self.converter.data[key] = {'json': {}, 'csv': [row]}
        result = self.converter.get_centro_de_costo(row, 'SubPlan')
        self.assertIsNone(result)
        # Check if an error was registered
        self.assertIn('1', self.converter.errs)
        self.assertNotIn('1', self.converter.succss)
        self.assertIn("[CSV] SubPlan no reconocido para centro de costo 'UNRECOGNIZED_VALUE'",
                          self.converter.data['1']['csv'][0]['Status'])


class TestTraslados(unittest.TestCase):
    def test_build_new_document_lines(self):
        data_sap = [
            {
                "DocEntry": 36663,
                "U_LF_Formula": "5214658",
                "BaseEntry": 2549,
                "BaseLine": 0,
                "LineStatus": "C",
                "ItemCode": "7795323000723",
                "Dscription": "FORTINI SABOR VAINILLA X 400 GR.",
                "StockPrice": 48728.440,
                "LineNum": 1,
                "Quantity": 1.0,
                "BatchNum": "111206790",
                "id__": 1
            },
            {
                "DocEntry": 36663,
                "U_LF_Formula": "5214658",
                "BaseEntry": 1154,
                "BaseLine": 0,
                "LineStatus": "C",
                "ItemCode": "7795323000723",
                "Dscription": "FORTINI SABOR VAINILLA X 400 GR.",
                "StockPrice": 42442.220,
                "LineNum": 0,
                "Quantity": 7.0,
                "BatchNum": "111206790",
                "id__": 2
            },
            {
                "DocEntry": 36663,
                "U_LF_Formula": "5214658",
                "BaseEntry": 1154,
                "BaseLine": 0,
                "LineStatus": "C",
                "ItemCode": "7795323000723",
                "Dscription": "FORTINI SABOR VAINILLA X 400 GR.",
                "StockPrice": 42442.220,
                "LineNum": 0,
                "Quantity": 1.0,
                "BatchNum": "1112067903301",
                "id__": 3
            }
        ]
        document_lines = [
            {
                "Price": 50526.0,
                "BaseLine": 0,
                "BaseType": "13",
                "ItemCode": "7795323000723",
                "Quantity": 9,
                "BaseEntry": 36663,
                "CostingCode": "SUC",
                "CostingCode2": "200",
                "CostingCode3": "EVNOPBSS",
                "StockInmPrice": 42442.22,
                "WarehouseCode": "200",
                "BatchNumbers": [
                    {
                        "Quantity": 8,
                        "BatchNumber": "1112067903301"
                    },
                    {
                        "Quantity": 1,
                        "BatchNumber": "111206790"
                    }
                ]
            }
        ]
        expected = [
                {
                    "BaseLine": 1,
                    "BaseType": "13",
                    "ItemCode": "7795323000723",
                    "Quantity": 1,
                    "BaseEntry": 36663,
                    "Price": 50526.0,
                    "StockInmPrice": 48728.44,
                    "CostingCode": "SUC",
                    "CostingCode2": "200",
                    "CostingCode3": "EVNOPBSS",
                    "WarehouseCode": "200",
                    "BatchNumbers": [
                        {
                            "Quantity": 1,
                            "BatchNumber": "111206790"
                        }
                    ],
                },
                {
                    "BaseLine": 0,
                    "BaseType": "13",
                    "ItemCode": "7795323000723",
                    "Quantity": 8,
                    "BaseEntry": 36663,
                    "Price": 50526.0,
                    "StockInmPrice": 42442.22,
                    "CostingCode": "SUC",
                    "CostingCode2": "200",
                    "CostingCode3": "EVNOPBSS",
                    "WarehouseCode": "200",
                    "BatchNumbers": [
                        {
                            "Quantity": 7,
                            "BatchNumber": "111206790"
                        },
                        {
                            "Quantity": 1,
                            "BatchNumber": "1112067903301"
                        }
                    ],
                }
            ]

        result = build_new_documentlines(data_sap, document_lines)

        self.assertTrue(result == expected)