import unittest
from unittest import TestCase

from base.models import PayloadMigracion
from base.tests.conf_test import make_instance, ProcessFakeSAP
from utils.converters import Csv2Dict
from utils.interactor_db import DBHandler, del_registro_migracion
from utils.parsers import Parser
from utils.pipelines import Validate, ProcessCSV, SaveInBD, ProcessSAP, ExcludeFromDB
from utils.resources import has_ceco


class TestsBaseAdvanced(TestCase):
    """Ejecuta el pipeline permitiendo que los tests sean ejecutados después
    que ha sido enviado a SAP y antes de ser eliminados los registros de la BD"""
    MODULE_NAME = ''
    SOURCE_FILE = ''

    @classmethod
    def setUpClass(cls):
        cls.module = make_instance(cls.MODULE_NAME, cls.SOURCE_FILE)
        cls.parser = Parser(cls.module, cls.module.filepath, 'TEST')
        cls.parser.pipeline = (Validate, ProcessCSV, SaveInBD, ProcessSAP)

        csv_to_dict, db, sap = cls.prepare_setupclass()
        cls.parser.run_filepath(csv_to_dict, db, sap)
        cls.result = csv_to_dict

    @classmethod
    def prepare_setupclass(cls):
        csv_to_dict = Csv2Dict(cls.module.name, cls.module.pk, cls.module.series, cls.module.sap)
        db = DBHandler(cls.module.migracion_id, csv_to_dict.name, csv_to_dict.pk)
        db.fname = cls.parser.input.stem
        sap = ProcessFakeSAP(cls.module)
        return csv_to_dict, db, sap

    @classmethod
    def tearDownClass(cls):
        del_registro_migracion(cls.module.migracion_id)
        # ExcludeFromDB().run(filename=cls.parser.input.stem, csv_to_dict=cls.result)

    def docs_in_db(self, *args):
        return PayloadMigracion.objects.only(*args).filter(
            modulo=self.MODULE_NAME,
            nombre_archivo=self.module.filepath.stem
        )

    def test_payload_in_db(self):
        """ Valida que el json generado sea el mismo que está en el campo payload en la bd"""
        docs_in_db = self.docs_in_db('valor_documento', 'payload')

        for k, v in self.result.data.items():
            doc = docs_in_db.get(valor_documento=k)
            with self.subTest(i=v):
                self.assertEqual(doc.payload, v['json'])

    def test_check_tags_in_status(self):
        """ Valida las etiquetas posibles en status """
        docs_in_db = self.docs_in_db('status')
        for doc in docs_in_db:
            tags_allowed = ('DocEntry', '[TIMEOUT]', '[CONNECTION]', '[SAP]', '[CSV]')
            with self.subTest(i=doc):
                self.assertTrue(any(tag in doc.status for tag in tags_allowed))

    def test_csv_error_shouldnt_go_to_sap(self):
        """ Valida si tiene error de CSV no debió enviarse a SAP """
        docs_in_db = self.docs_in_db('valor_documento', 'status', 'payload', 'enviado_a_sap')

        for k, v in self.result.data.items():
            doc = docs_in_db.get(valor_documento=k)
            with self.subTest(i=v):
                if '[CSV]' in doc.status:
                    self.assertFalse(doc.enviado_a_sap, msg=f'{doc.valor_documento} status={doc.status}')
                else:
                    self.assertTrue(doc.enviado_a_sap, msg=f'{doc.valor_documento} status={doc.status}')

    def test_if_has_ceco_391_then_docentry_noaplica(self):
        """ Valida que la información enviada a SAP tenga 'DocEntry: No aplica' cuando tenga ceco 391"""
        docs_in_db = self.docs_in_db('status', 'payload').filter(enviado_a_sap=True)

        for doc in docs_in_db:
            with self.subTest(i=doc):
                if has_ceco(self.MODULE_NAME, doc.payload, '391'):
                    self.assertTrue(doc.status == 'DocEntry: No aplica')

    def test_consistency_in_status_in_lines(self):
        """ Valida que el status sea igual tanto en status como en la columna línea"""
        docs_in_db = self.docs_in_db('valor_documento', 'status', 'lineas')

        for doc in docs_in_db:
            lines = eval(doc.lineas)
            for line in lines:
                with self.subTest(k=line):
                    self.assertEqual(doc.status, line['Status'])


class DocumentLinesTestsMixin:
    """Clase a ser heredada en la suíte de tests. Por aquellos escenarios que usen DocumentLines"""

    def test_consistency_in_document_lines(self):
        """ Valida que tenga tantos dicts en DocumentLines como lineas reconocidas del archivo."""
        for k, v in self.result.data.items():
            with self.subTest(i=v):
                if self.MODULE_NAME != 'facturacion':
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
                        if not art['Quantity']:
                            self.assertTrue(k in self.result.errs)
                        else:
                            self.assertEqual(
                                art['Quantity'],
                                sum(art['Quantity'] for art in art['BatchNumbers'])
                            )


class CustomTestsMixin:
    """Clase a ser heredada en la suíte de tests.
    Aquí constan todos los tests que se puedan aplicar a
    las clases que la hereden """

    DONT_USE_COSTINGCODE3 = ('ajustes_entrada', 'ajustes_salida', 'compras',
                             'ajustes_vencimiento_lote', 'pagos_recibidos')

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
        """ Revisa que haya contenido en la llave Status si hubo error en ese documento."""
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
