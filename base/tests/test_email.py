"""
<img title="[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para contrato 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para contrato 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para contrato 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para contrato 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para contrato 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan no reconocido para contrato 'Capita Nueva EPS Disfarma'" src="https://qnzzuv.stripocdn.email/content/guids/CABINET_67ebc0d6c23a39a9dedda41faadd46f40cadf47dbfd56f5d215193d2eb43273f/images/close.png" style="display:block;font-size:14px;border:0;outline:none;text-decoration:none" width="25">
"""
import pickle
import unittest

from base.templatetags.filter_extras import make_text_status
from core.settings import BASE_DIR
from utils.resources import Email


class TestEmail(unittest.TestCase):

    def test_filter_make_text_status(self):
        data = [{'Beneficiario': 'JANE DOE', 'CECO': '306', 'CantidadDispensada': '30.0',
                 'CategoriaActual': '6', 'FechaDispensacion': '2023-10-03 20:53:48',
                 'Lote': 'D00351A', 'Mipres': '', 'NIT': '1234567890',
                 'NroAutorizacion': '', 'NroDocumento': '9090909090', 'NroSSC': '118',
                 'Plan': 'Regimen Subsidiado', 'Plu': '7705959015152', 'Precio': '0.0',
                 'Status': "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva "
                           "EPS Disfarma' | [CSV] SubPlan no reconocido para contrato 'Capita "
                           "Nueva EPS Disfarma'",
                 'SubPlan': 'Capita Nueva EPS Disfarma', 'TipodeIdentidad': 'CC',
                 'UsuarioDispensa': '123123123'},
                {'Beneficiario': 'JANE DOE', 'CECO': '306', 'CantidadDispensada': '30.0',
                 'CategoriaActual': '6', 'FechaDispensacion': '2023-10-03 20:53:48',
                 'Lote': '1466035', 'Mipres': '', 'NIT': '1234567890',
                 'NroAutorizacion': '', 'NroDocumento': '9090909090', 'NroSSC': '118',
                 'Plan': 'Regimen Subsidiado', 'Plu': '7703153035044', 'Precio': '0.0',
                 'Status': "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva "
                           "EPS Disfarma' | [CSV] SubPlan no reconocido para contrato 'Capita "
                           "Nueva EPS Disfarma'",
                 'SubPlan': 'Capita Nueva EPS Disfarma', 'TipodeIdentidad': 'CC',
                 'UsuarioDispensa': '123123123'},
                {'Beneficiario': 'JANE DOE', 'CECO': '306', 'CantidadDispensada': '30.0',
                 'CategoriaActual': '6', 'FechaDispensacion': '2023-10-03 20:53:48',
                 'Lote': 'DXL0500', 'Mipres': '', 'NIT': '1234567890',
                 'NroAutorizacion': '', 'NroDocumento': '9090909090', 'NroSSC': '118',
                 'Plan': 'Regimen Subsidiado', 'Plu': '7705959880361', 'Precio': '0.0',
                 'Status': "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva "
                           "EPS Disfarma' | [CSV] SubPlan no reconocido para contrato 'Capita "
                           "Nueva EPS Disfarma'",
                 'SubPlan': 'Capita Nueva EPS Disfarma', 'TipodeIdentidad': 'CC',
                 'UsuarioDispensa': '123123123'},
                {'Beneficiario': 'JANE DOE', 'CECO': '306', 'CantidadDispensada': '2.0',
                 'CategoriaActual': '6', 'FechaDispensacion': '2023-10-03 20:53:48',
                 'Lote': '3H6762', 'Mipres': '', 'NIT': '1234567890',
                 'NroAutorizacion': '', 'NroDocumento': '9090909090', 'NroSSC': '118',
                 'Plan': 'Regimen Subsidiado', 'Plu': '7702057801793', 'Precio': '0.0',
                 'Status': "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva "
                           "EPS Disfarma' | [CSV] SubPlan no reconocido para contrato 'Capita "
                           "Nueva EPS Disfarma'",
                 'SubPlan': 'Capita Nueva EPS Disfarma', 'TipodeIdentidad': 'CC',
                 'UsuarioDispensa': '123123123'},
                {'Beneficiario': 'JANE DOE', 'CECO': '306', 'CantidadDispensada': '60.0',
                 'CategoriaActual': '6', 'FechaDispensacion': '2023-10-03 20:53:48', 'Lote': 'CCL3496',
                 'Mipres': '', 'NIT': '1234567890', 'NroAutorizacion': '',
                 'NroDocumento': '9090909090', 'NroSSC': '118', 'Plan': 'Regimen Subsidiado',
                 'Plu': '7705959882129', 'Precio': '0.0',
                 'Status': "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva "
                           "EPS Disfarma' | [CSV] SubPlan no reconocido para contrato 'Capita "
                           "Nueva EPS Disfarma'",
                 'SubPlan': 'Capita Nueva EPS Disfarma', 'TipodeIdentidad': 'CC',
                 'UsuarioDispensa': '123123123'},
                {'Beneficiario': 'JANE DOE', 'CECO': '306', 'CantidadDispensada': '30.0',
                 'CategoriaActual': '6', 'FechaDispensacion': '2023-10-03 20:53:48', 'Lote': 'D060658',
                 'Mipres': '', 'NIT': '1234567890', 'NroAutorizacion': '', 'NroDocumento': '9090909090',
                 'NroSSC': '118', 'Plan': 'Regimen Subsidiado', 'Plu': '7702184011829', 'Precio': '0.0',
                 'Status': "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva "
                           "EPS Disfarma' | [CSV] SubPlan no reconocido para contrato 'Capita "
                           "Nueva EPS Disfarma'",
                 'SubPlan': 'Capita Nueva EPS Disfarma', 'TipodeIdentidad': 'CC',
                 'UsuarioDispensa': '123123123'}]

        result = make_text_status(data)
        self.assertEqual(result,
                         "[CSV] SubPlan no reconocido para centro de costo 'Capita Nueva EPS Disfarma', [CSV] SubPlan "
                         "no reconocido para contrato 'Capita Nueva EPS Disfarma'")

    def test_sending_email(self):
        import os;
        os.environ['DJANGO_SETTINGS_MODULE'] = 'core.settings';
        import django;
        django.setup();
        with open(BASE_DIR / 'dispensacion_data.pkl', 'rb') as fp:
            data = pickle.load(fp)

        with open(BASE_DIR / 'dispensacion_module.pkl', 'rb') as fp:
            module = pickle.load(fp)

        e = Email(module, data)
        # e.send()
        e.render_local_html('basex')
