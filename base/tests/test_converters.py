from unittest import TestCase, mock
from utils.converters import Csv2Dict

class TestCsv2Dict(TestCase):
    def setUp(self):
        self.sap_mock = mock.MagicMock()
        self.converter = Csv2Dict(
            name="test_converter",
            pk="ID",
            series={},
            sap=self.sap_mock
        )

    @mock.patch('utils.converters.log')
    def test_transform_date_scenarios(self, mock_log):
        """Test transform_date with various scenarios using subtests."""
        test_cases = [
            {
                "date_input": "2022-12-31 18:36:00",
                "kwargs": {},
                "expected": "20221231"
            },
            {
                "date_input": "2022-12-31 18:36:00",
                "kwargs": {"add_time": True},
                "expected": "202212311836"
            },
            {
                "date_input": "2026-11-21",
                "kwargs": {"force_exception": False},
                "expected": "20261121"
            },
            {
                "date_input": "invalid-date",
                "kwargs": {"force_exception": False},
                "expected": None
            },
        ]

        for case in test_cases:
            with self.subTest(date_input=case["date_input"], kwargs=case["kwargs"]):
                row = {"ID": "1", "Fecha": case["date_input"], "Status": ""}
                # Clear errors for each subtest
                self.converter.errs.clear()
                
                result = self.converter.transform_date(row, "Fecha", **case["kwargs"])
                
                self.assertEqual(result, case["expected"])
                self.assertEqual(row["Status"], "")
                self.assertNotIn("1", self.converter.errs)
                mock_log.error.assert_not_called()
                
                # Reset mock
                mock_log.reset_mock()

    @mock.patch('utils.converters.log')
    def test_transform_date_invalid_format(self, mock_log):
        """Test invalid date format triggers error registration."""
        row = {"ID": "1", "Fecha": "invalid-date", "Status": ""}
        # Setup data for reg_error to function correctly
        self.converter.data["1"] = {'csv': [row]}
        
        result = self.converter.transform_date(row, "Fecha")
        
        self.assertIsNone(result)
        self.assertIn("[CSV] Formato inesperado en Fecha", row["Status"])
        self.assertIn("1", self.converter.errs)
        mock_log.error.assert_called()
