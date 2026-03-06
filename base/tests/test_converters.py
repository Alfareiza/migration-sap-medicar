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


class TestUpdateStatus(TestCase):
    def setUp(self):
        self.sap_mock = mock.MagicMock()
        self.converter = Csv2Dict(
            name="test_converter",
            pk="Lote",
            series={},
            sap=self.sap_mock
        )

    def test_status_propagation_on_new_error(self):
        """
        Tests that a new status is propagated to all rows if the key is marked as an error.
        """
        key = "DOC1"
        new_status = "A new, more important error"

        # Initial state with some rows already having a status
        self.converter.data = {
            key: {
                'csv': [
                    {self.converter.pk: key, 'Status': 'Initial error'},
                    {self.converter.pk: key, 'Status': ''},
                ]
            }
        }
        self.converter.errs = {key}

        # A new row with a new status is added
        current_row_with_new_status = {self.converter.pk: key, 'Status': new_status}
        self.converter.data[key]['csv'].append(current_row_with_new_status)

        # The function under test is called
        self.converter.update_status_necessary_columns(current_row_with_new_status, key)

        # All rows should now have the new status
        for r in self.converter.data[key]['csv']:
            self.assertEqual(r['Status'], new_status)

    def test_no_status_change_when_no_error_present(self):
        """
        Tests that status is not updated if the key is not in the error set.
        """
        key = "DOC2"
        initial_rows = [
            {self.converter.pk: key, 'Status': ''},
            {self.converter.pk: key, 'Status': 'Some info'},
        ]

        self.converter.errs = set()
        self.converter.data = {
            key: {
                'csv': list(initial_rows)
            }
        }

        # A new row is added
        current_row = {self.converter.pk: key, 'Status': ''}
        self.converter.data[key]['csv'].append(current_row)

        # The function under test is called
        self.converter.update_status_necessary_columns(current_row, key)

        # The statuses of the initial rows should not have changed
        self.assertEqual(self.converter.data[key]['csv'][0]['Status'], initial_rows[0]['Status'])
        self.assertEqual(self.converter.data[key]['csv'][1]['Status'], initial_rows[1]['Status'])
        # The new row's status should also be unchanged
        self.assertEqual(self.converter.data[key]['csv'][2]['Status'], '')

    def test_update_status_from_previous_row_when_current_is_empty(self):
        """
        Tests that if a row has an empty status, but the key is in self.errs,
        its status is updated from the previous row's status.
        """
        key = '4V660'
        initial_status = "[CSV] No fue encontrado AbsEntry para lote '4V660'"

        # Setup initial state as requested by the user
        self.converter.data = {
            key: {
                'csv': [{'FechaVencimiento': '2025-05-30', 'Lote': '4V660', 'Plu': '7707288822951',
                         'Status': initial_status}],
                'json': {'ExpirationDate': '20250530', 'Series': None}
            }
        }
        self.converter.errs = {key}

        # Simulate adding a new row with empty status
        new_row = {self.converter.pk: key, 'FechaVencimiento': '2025-06-30', 'Lote': '4V660', 'Plu': '7707288822951', 'Status': ''}
        self.converter.data[key]['csv'].append(new_row)

        # Call the function under test
        self.converter.update_status_necessary_columns(new_row, key)

        # Assertions
        self.assertEqual(self.converter.data[key]['csv'][0]['Status'], initial_status)
        self.assertEqual(self.converter.data[key]['csv'][1]['Status'], initial_status)

    def test_update_status_necessary_columns_when_remaining_rows_is_one(self):
        """Tests that update_status_necessary_columns correctly handles the case where
        a key exists in self.errs and a new row needs its status synchronized with
        existing rows for that key.
        """
        key = '4V660'
        self.converter.data = {
            key:
                {'csv': [
                    {'FechaVencimiento': '2025-05-30', 'Lote': '4V660', 'Plu': '7707288822951',
                     'Status': "[CSV] No fue encontrado AbsEntry para lote '4V660'"}
                ],
                    'json': {'ExpirationDate': '20250530', 'Series': None}}}
        self.converter.errs = {key}
        new_row = {'FechaVencimiento': '2026-05-30', 'Lote': '4V660', 'Plu': '7707288822951', 'Status': ''}

        self.converter.update_status_necessary_columns(new_row, key)
