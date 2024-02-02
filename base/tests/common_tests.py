class CustomTestsMixin:
    """Clase a ser heredada en la suíte de tests.
    Aquí constan todos los tests que se puedan aplicar a
    las clases que la hereden """

    DONT_USE_COSTINGCODE3 = ('ajustes_entrada', 'ajustes_salida',)

    def get_costing_code_3(self, subplan):
        match subplan:
            case "CAPITA":
                return "CAPSUB01"
            case "CAPITA NUEVA EPS DISFARMA":
                return "CAPSUB01"
            case "CAPITA COMPLEMENTARIA SUBSIDIADO":
                return "CAPSUB01"
            case "CAPITA SUBSIDIADO":
                return "CAPSUB01"
            case "CAPITA CONTRIBUTIVO":
                return "CAPCON01"
            case "CAPITA COMPLEMENTARIA CONTRIBUTIVO":
                return "CAPCON01"
            case "EVENTO PBS CONTRIBUTIVO":
                return "EVPBSCON"
            case "EVENTO NO PBS SUBSIDIADO":
                return "EVNOPBSS"
            case "EVENTO NO PBS CONTRIBUTIVO":
                return "EVPBSCON"
            case "EVENTO PBS SUBSIDIADO":
                return "EVPBSSUB"

    def test_status_emptiness(self):
        """ Revisa que haya contenido en la columna Status si hubo error en ese documento."""
        for k, v in self.result.data.items():
            for line in v['csv']:
                with self.subTest(i=v):
                    if k in self.result.errs:
                        self.assertTrue(line['Status'])
                    else:
                        self.assertFalse(line['Status'])

    def test_costing_code_three(self):
        """Valida que el costingcode3 sea el esperado. Obs.: Usado en modulos que tienen SubPlan"""
        if self.MODULE_NAME not in self.DONT_USE_COSTINGCODE3:
            for k, v in self.result.data.items():
                subplan = v['csv'][0]['SubPlan'].upper()
                for art in v['json']['DocumentLines']:
                    with self.subTest(i=v):
                        self.assertEqual(
                            self.get_costing_code_3(subplan),
                            art['CostingCode3']
                        )


class DocumentLinesTestsMixin:
    """Clase a ser heredada en la suíte de tests. Por aquellos escenarios que usen DocumentLines"""

    def test_consistency_in_document_lines(self):
        """ Valida que tenga tantos dicts en DocumentLines como lineas reconocidas del archivo."""
        for k, v in self.result.data.items():
            if self.MODULE_NAME != 'facturacion':
                with self.subTest(i=v):
                    qty_articles = sum(len(art['BatchNumbers']) for art in v['json']['DocumentLines'])
                    self.assertEqual(qty_articles, len(v['csv']))
            else:
                self.assertEqual(len(v['json']['DocumentLines']), len(v['csv']))

    def test_consistency_in_batch_numbers(self):
        """ Valida que la cantidad que aparece en el DocumentLines sea igual a
        la suma de las cantidades de los BatchNumbers"""
        if self.MODULE_NAME != 'facturacion':
            for k, v in self.result.data.items():
                for art in v['json']['DocumentLines']:
                    with self.subTest(i=v):
                        self.assertEqual(art['Quantity'], sum(art['Quantity'] for art in art['BatchNumbers']))
