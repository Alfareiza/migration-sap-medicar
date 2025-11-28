from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from django.conf import settings

from base.templatetags.filter_extras import make_text_status
from core.settings import logger as log
from utils.decorators import logtime
from utils.resources import (
    format_number as fn,
    is_later_than_january_31_2024,
    load_comments,
    string_to_datetime
)
from utils.sap.manager import SAPData


@dataclass
class Csv2Dict:
    name: str
    pk: str
    series: dict
    sap: SAPData
    data: dict = field(init=False, default_factory=dict)
    errs: set = field(init=False, default_factory=set)
    succss: set = field(init=False, default_factory=set)
    csv_lines: int = 0

    def __repr__(self):
        return (f"Csv2Dict(name='{self.name}', "
                f"{self.pk}={len(self.data.values())} series={self.series} "
                f"csv_lines={self.csv_lines})")

    @property
    def succss_ordered_by_date(self) -> Iterable:
        """ Ordena los info.success por el campo fecha que posea el modulo """
        fecha_field = [
            h
            for h in tuple(getattr(settings, f"{self.name.upper()}_HEADER", []))
            if 'fecha' in h.lower()
        ]
        try:
            # fecha_field son todos los campos del header del modulo que tengan la palabra 'fecha'
            if not fecha_field:
                raise Exception(f'No se encontró header con la palabra \'fecha\' en {self.name.uppper()}_HEADER')
            order_func = lambda d: datetime.strptime(self.data[d]['csv'][0][fecha_field[0]], '%Y-%m-%d %H:%M:%S')
            return sorted(self.succss.intersection(self.data), key=order_func)
        except Exception as e:
            log.error(f'Error al ordenar la info en {self.name.upper()!r}: {repr(e)}')
            return self.succss

    def load_data_from_db(self, records) -> None:
        for record in records:
            record.refresh_from_db()
            # log.info(f"{record.valor_documento} Cargando en csvdict actual DL -> {record.payload['DocumentLines']}")
            self.data[record.valor_documento] = {
                'json': record.payload,
                'csv': eval(record.lineas),
            }
            # log.info(f"{record.valor_documento} Nuevo DL en csvdict           -> {self.data[record.valor_documento]['json']['DocumentLines']}")
            self.csv_lines += record.cantidad_lineas_documento
            if record.status == '' or 'DocEntry' in record.status:
                self.succss.add(record.valor_documento)
            else:
                self.errs.add(record.valor_documento)

    def clear_data(self):
        self.data.clear()
        self.errs.clear()
        self.succss.clear()
        self.csv_lines = 0

    def group_by_type_of_errors(self):
        """
        Crea los atributos csv_errs, sap_errs y other_errs donde se guardan las
        referencias de los registros que tienen ese tipo de error.
        Los cuáles podrán ser indexados en self.data.
        """
        self.result_succss, self.csv_errs, self.sap_errs, self.other_errs = {}, {}, {}, {}
        for k, v in self.data.items():
            status_text = make_text_status(v['csv'])

            if 'CSV' in status_text:
                if status_text not in self.csv_errs:
                    self.csv_errs[status_text] = []
                self.csv_errs[status_text].append(k)

            elif 'SAP' in status_text:
                if status_text not in self.sap_errs:
                    self.sap_errs[status_text] = []
                self.sap_errs[status_text].append(k)

            elif 'CONNECTION' in status_text or 'TIMEOUT' in status_text:
                if status_text not in self.sap_errs:
                    self.other_errs[status_text] = []
                self.other_errs[status_text].append(k)

            elif 'DocEntry' in status_text:
                self.result_succss[k] = status_text

            elif status_text:
                log.error(f"{status_text!r} no es un tipo de error válido, "
                          f"los errores deben ser o tipo SAP o tipo CSV.")

    def reg_error(self, row, txt):
        """Agrega el motivo del error a la llave 'Status'."""
        self.errs.add(row[f'{self.pk}'])
        try:
            self.succss.remove(row[f'{self.pk}'])
        except Exception:
            ...
        if row['Status'] == '':
            row['Status'] = txt
        elif txt not in row['Status']:
            row['Status'] += f" | {txt}"

        self.update_status_necessary_columns(row, row[self.pk])

    def get_series(self, row):
        """Determina el series a partir del SubPlan o No y crea la variable single_serie."""
        # TODO En el caso de facturación viene 'Capita complementaria Subsidiado '
        #  en la columna de subplan y por ende entra en el primer if y quiebra el
        #  código porque para facturación 'CAPITA' no existe en el dict self.series
        subplan = row.get('SubPlan', '').upper()
        if 'CAPITA' in subplan or 'MAGISTERIO' in subplan:
            self.single_serie = self.series['CAPITA']
            return self.series['CAPITA']
        elif 'EVENTO' in subplan:
            self.single_serie = self.series['EVENTO']
            return self.series['EVENTO']
        else:
            self.single_serie = 99  # Serie a modo de joker
            txt = f"[CSV] No reconocido 'CAPITA' o 'EVENTO' en SubPlan {row.get('SubPlan')!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)

    def get_contrato(self, row: dict) -> str:
        """
        Define el CostingCode3 a partir del SubPlan
        :param row: Diccionario con datos que vienen del csv.
        :return: Codigo del centro.
        """
        match row.get('SubPlan', '').upper().strip():
            case "CAPITA" | "CAPITA NUEVA EPS DISFARMA" | "CAPITA COMPLEMENTARIA SUBSIDIADO" | "CAPITA SUBSIDIADO" | "CAPITA BASICA SUBSIDIADO":
                return "CAPSUB01"
            case "CAPITA CONTRIBUTIVO" | "CAPITA COMPLEMENTARIA CONTRIBUTIVO" | "CAPITA BASICA CONTRIBUTIVO":
                return "CAPCON01"
            case "EVENTO NO PBS CONTRIBUTIVO" | "EVENTO PBS CONTRIBUTIVO" | "EVENTO CONTRIBUTIVO" | "EVENTO PBS CONTRIBUTIVO SIN AUTORIZACION":
                return "EVPBSCON"
            case "EVENTO NO PBS SUBSIDIADO":
                return "EVNOPBSS"
            case "EVENTO PBS SUBSIDIADO" | "EVENTO SUBSIDIADO" | "EVENTO PBS SUBSIDIADO SIN AUTORIZACION":
                return "EVPBSSUB"
            case "MAGISTERIO MEDIFARMA EVENTO" | "MAGISTERIO RAMEDICAS CAPITA" | "MAGISTERIO FARMAT EVENTO":
                return "MAGIS"
            case "":
                return ""
            case _:
                txt = f"[CSV] SubPlan no reconocido para contrato {row.get('SubPlan')!r}"
                log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
                self.reg_error(row, txt)

    def get_centro_de_costo(self, row: dict, column_name: str, tipo_ajuste=None) -> str:
        """
        Define el AccountCode a partir de un conjunto de constantes
        :param row: Diccionario con datos que vienen del csv.
        :param column_name: Columna a ser considerada.
        :param tipo_ajuste: Puede ser 'entrada', o 'salida'
        :return: Centro de costo definido por contabilidad.
        """
        # match row.get('SubPlan', '').upper():
        match row.get(column_name, '').upper().strip():
            case "CAPITA" | "CAPITA SUBSIDIADO" | "CAPITA NUEVA EPS DISFARMA" | "CAPITA COMPLEMENTARIA SUBSIDIADO" | "CAPITA BASICA SUBSIDIADO":
                return "7165950102"
            case "CAPITA CONTRIBUTIVO" | "CAPITA COMPLEMENTARIA CONTRIBUTIVO":
                return "7165950101"
            case "EVENTO PBS CONTRIBUTIVO" | "CAPITA BASICA CONTRIBUTIVO" | "EVENTO SUBSIDIADO" | "EVENTO PBS SUBSIDIADO SIN AUTORIZACION":
                return "7165950202"
            case "EVENTO NO PBS SUBSIDIADO":
                return "7165950203"
            case "EVENTO NO PBS CONTRIBUTIVO":
                return "7165950204"
            case "MAGISTERIO MEDIFARMA EVENTO" | "MAGISTERIO RAMEDICAS CAPITA" | "MAGISTERIO FARMAT EVENTO":
                return "7165950401"
            case "EVENTO PBS SUBSIDIADO" | "EVENTO CONTRIBUTIVO" | "EVENTO PBS CONTRIBUTIVO SIN AUTORIZACION":
                return "7165950201"
            case "AJUSTE POR FALTANTE":  # Estaba FALTANTES
                return "7165950301"
            case "AJUSTE POR SOBRANTE":  # Estaba SOBRANTES
                return "7165950302"
            case "AJUSTE EN INVENTARIO GENERAL":
                if tipo_ajuste == 'salida':
                    return "7165950301"
                elif tipo_ajuste == 'entrada':
                    return "7165950302"
            case "AVERIAS":
                return "5310350102"
            case "SALIDA POR DONACION" | "ENTRADA POR DONACION":
                return "7165950303"
            case "VENCIDOS":
                return "5310350102"
            case "":
                return ""
            case _:
                txt = f"[CSV] {column_name} no reconocido para centro de costo {row.get(column_name)!r}"
                log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
                self.reg_error(row, txt)

    def get_costing_code(self, row, column_name='CECO') -> str:
        """
        Determina el CostingCode apartir del CECO.
        CECO es un número que se consulta en API de SAP
        y del resultado se toma el U_HBT_Dimension1
        :return:
        """
        try:
            ceco = row[column_name]
            costing_code = self.sap.get_costing_code_from_sucursal(ceco)
            if not costing_code:
                raise Exception()
        except Exception:
            txt = f"[CSV] CECO no reconocido {ceco!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return costing_code

    def get_plan(self, row: dict) -> str:
        """Determina si es subsidiado o contributivo"""
        plan = row.get('Plan', '').upper()
        if 'SUBSIDIADO' in plan or 'Capita' in plan or 'MAGISTERIO' in plan:
            return 'S'
        elif 'CONTRIBUTIVO' in plan:
            return 'C'
        elif plan == '':
            # Si entra aqui es porque el csv no tiene la columna Plan
            # Esto puede pasar si se esta procesando un modulo que no trabaje con Plan
            return ''
        else:
            txt = f"No fue detectado ni contributivo ni subsidiado en {plan!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. No fue detectado "
                      f"ni contributivo ni subsidiado en {plan!r}")
            self.reg_error(row, txt)

    def transform_date_v2(self, row: dict, column_name: str) -> str:
        """Transforma la fecha del formato 30/11/2024 a 20221231
        Obs.: Solamente usado para pruebas, por ajustes_entradas_prueba
        """
        try:
            dt = row[column_name]
            anho, mes, dia = dt.split('-')
        except Exception:
            log.error(f"{self.pk} {row[f'{self.pk}']}. Fecha '{dt}' no pudo ser transformada. "
                      f'Se esperaba el formato "31/12/2023" y se recibió {dt}')
            self.reg_error(row,
                           f'[CSV] Formato inesperado en {column_name} se espera este formato -> "31/12/2023"')
        else:
            return f"{anho}{mes}{dia}"

    def transform_date(self, row: dict, column_name: str, force_exception=True) -> str:
        """ Transforma la fecha del formato "2022-12-31 18:36:00" a "20221231" """
        try:
            dt = row[column_name]
            anho, mes, dia = dt.split(' ')[0].split('-')
        except Exception:
            if force_exception:
                log.error(f"{self.pk} {row[f'{self.pk}']}. Fecha '{dt}' no pudo ser transformada. "
                          f'Se esperaba el formato "2022-12-31 18:36:00" y se recibió {dt}')
                self.reg_error(row, f'[CSV] Formato inesperado en {column_name} '
                                    f'se espera este formato -> 2022-12-31 18:36:00')
            else:
                ...
        else:
            return f"{anho}{mes}{dia}"

    def generate_idssc(self, row: dict, doc_date: str) -> str:
        """
        Usado en el módulo de dispensación, genera un código único por
        registro, que contiene el NroSSC concatenado con el doc_date
        :param row: Linea del csv leida en determinado momento.
        :param doc_date: Fecha ya convertida en formato reconocido por sap
                        Ej.: '20230719'
        :return:
        """
        nro_ssc = row.get(self.pk, '')
        if not nro_ssc and not doc_date:
            log.error(f"{self.pk} {nro_ssc}. No reconocido")
            self.reg_error(nro_ssc, "[CSV] No fue posible crear el IDSSC")

        return f"{doc_date}{nro_ssc}"

    def get_codigo_tercero(self, row: dict) -> str:
        """ Determina el codigo del nit, agregándole CL al comienzo del valor recibido """
        nit = row.get('NIT', '')
        if nit != '':
            return f"CL{nit}"

        log.error(f"{self.pk} {row[f'{self.pk}']}. NIT no reconocido : {nit!r}")
        self.reg_error(row, f"[CSV] NIT no reconocido: {nit!r}")

    def get_nit_compras(self, row: dict) -> str:
        """Determina el codigo del nit, agregándole PR al
        comienzo del valor recibido."""
        nit = row.get('NIT', '')
        if nit != '':
            return f"PRV{nit}"
        log.error(f"{self.pk} {row[f'{self.pk}']}. NIT no reconocido : {nit!r}")
        self.reg_error(row, f"[CSV] NIT no reconocido: {nit!r}")

    def get_nombre_afiliado(self, row: dict) -> str:
        person = row.get("Beneficiario", '')
        if person != '':
            return person
        log.error(f"{self.pk} {row[f'{self.pk}']}. Beneficiario no reconocido : {person!r}")
        self.reg_error(row, f"[CSV] Beneficiario no reconocido: {person!r}")

    def make_float(self, row, colum_name) -> float:
        """ Intenta convertir valor a decimal."""
        try:
            vlr = float(row[colum_name])
        except Exception:
            txt = f"[CSV] {colum_name} '{row[colum_name]}' " \
                  "no pudo ser convertido a decimal."
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return vlr

    def make_int(self, row, colum_name) -> int:
        """ Intenta convertir valor a decimal."""
        try:
            num = int(float(row[colum_name]))  # En dispensacion este valor llega así: '9.'0
        except Exception:
            txt = (f"[CSV] {colum_name} {row[colum_name]!r} "
                   "no pudo ser convertido a numero entero.")
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return num

    def get_num_aut(self, row):
        """
        Obtiene el numero de autorización.
        Cuando es evento, se intenta convertir a int
        Sino, retorna un string vacío.
        """
        is_evento = self.single_serie == self.series.get('EVENTO')
        subplan = row.get('SubPlan', '').upper().strip()
        if subplan in ("EVENTO PBS CONTRIBUTIVO SIN AUTORIZACION",
                       "EVENTO PBS SUBSIDIADO SIN AUTORIZACION"):
            return ''
        return self.make_int(row, "NroAutorizacion") if is_evento else ''

    def get_plu(self, row, column_name='Plu'):
        item_code = row.get(column_name, '')
        if item_code != '':
            return item_code
        log.error(f"{self.pk} {row[f'{self.pk}']}. Plu no reconocido : {item_code!r}")
        self.reg_error(row, f"[CSV] Plu no reconocido: {item_code!r}")

    def get_iva_code(self, row):
        """Busca el IVA en Columna y devuelve el código correspondiente"""
        iva_code = row.get('IVA', '')

        match iva_code:
            case '0.0':
                return 'NOGRAVADOS'
            case '19.0':
                return 'GRAVADOS19'
            case _:
                txt = f"Detectado IVA inválido {iva_code!r}"
                log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
                self.reg_error(row, txt)

    def pending_to_implement(self, row, msg):
        txt = f"[CSV] {msg}. {row[self.pk]!r}"
        log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
        self.reg_error(row, txt)

    def get_doc_entry_factura(self, row) -> str:
        """Busca el doc entry de la factura correspondiente."""
        try:
            dentry = self.sap.get_docentry_factura(row[self.pk])
            if not dentry:
                raise Exception()
        except Exception:
            txt = f"[CSV] No se encontró dispensación para SSC {row[self.pk]!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return dentry

    def get_info_sap_entrega(self, row, to_reach):
        """
        Busca el doc entry (registro en sap) de la entrega correspondiente.
        filtrando exactamente con el ItemCode del row
        Usado por NOTAS_CREDITO
        :param: to_reach: es el valor a tomarse del resultado de la consulta
        hecha en SAP en el endpoint InfoEntregaQuery.
        Ex.: Puede ser cualquier key del siguiente diccionário.
                 {
                     DocEntry": 11532,
                    "Dscription": "LANCETAS DESECHABLES UND",
                    "BaseLine": null,
                    "U_LF_Formula": "3822612",
                    "ItemCode": "7703153039042",
                    "DocEntry_1": 11532,
                    "LineStatus": "O",
                    "StockPrice": 24.0,
                }
        """
        try:
            if not row['Plu'].isnumeric():
                raise Exception('Plu no valido al ser consultado con SSC')
            if info := self.sap.get_info_ssc(row[self.pk]):
                res = list(filter(lambda v: v['ItemCode'] == row['Plu'], info))
            if not info:
                raise Exception('No se encontraron entregas para SSC')
            if not res:
                raise Exception(f"No se encontro el Plu {row['Plu']} en SSC")
        except Exception as e:
            txt = f"[CSV] {e} {row[self.pk]}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return res[0][to_reach]

    def get_bin_abs_entry(self, row, colum_name):
        """Busca el BinAbsEntry en SAP de la bodega correspondiente.
        colum_name puede ser Bodega Origen o Bodega Destino"""
        try:
            if binentry := self.sap.get_bin_abs_entry_from_ceco(row[colum_name], colum_name.lower()):
                res = binentry
            else:
                raise Exception()
        except Exception:
            txt = f"[CSV] No fue encontrado BinEntry para {colum_name} {row[colum_name]!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return res

    def get_num_in_buy(self, row, qty):
        """
        Busca el NumInBuy en SAP del Plu correspondiente,
        Luego se calcula la cantidad  entre NumInBuy (embalaje).
            Ej.: 60 (cantidad) / 20 (NumInBuy) = 3 (Valor a retonar)
        """
        res = None
        try:
            if not row['Plu'].isnumeric():
                raise Exception('Plu no numerico. No pudo ser consultado su embalaje en SAP')
            if info := self.sap.get_embalaje_info_from_plu(row['Plu']):
                res_api = int(info[0]['NumInBuy'])
                residuo, cociente = divmod(qty, res_api)
                if cociente:
                    raise Exception(f"Plu {row['Plu']} presenta inconsistencia "
                                    f"con cantidad {qty} siendo {res_api} su embalaje (NumInBuy).")
                res = residuo
            if not info:
                raise Exception(f"No se encontraron embalajes para el Plu {row['Plu']!r}")
        except Exception as e:
            txt = f"[CSV] {e}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return res

    def get_abs_entry_from_lote(self, row):
        try:
            if binentry := self.sap.get_bin_abs_entry_from_lote(row['Lote']):
                res = binentry
            else:
                raise Exception()
        except Exception:
            txt = f"[CSV] No fue encontrado AbsEntry para lote {row['Lote']!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return res

    @logtime('CSV')
    def process(self, csv_reader):
        log.info(f"[{self.name}] Comenzando procesamiendo de CSV.")
        self.process_module(csv_reader)
        log.info(f"[{self.name}] CSV procesado con éxito, {fn(self.csv_lines)} líneas leidas,"
                 f" {fn(len(self.succss))} payloads creados y {fn(len(self.errs))} Errores de CSV.")

    def build_payment_invoices(self, row):
        # El DocEntry se rellena haciendo una consulta
        # a SAP con el numero de la factura de medicar (NumAtCard)
        return {
            "LineNum": 0,  # TODO: Preguntar a Marlay
            "DocEntry": 13,  # TODO: DocEntry de la factura a la que se le aplica el pago
            "SumApplied": 50000,  # TODO: Preguntar a Marlay Cual es la diferencia con CashSum?
            "InvoiceType": "it_Invoice"
        }

    def build_stock_transfer_lines(self, row):
        return {  # Este es el cuerpo de un traslado de un Plu con uno o más lotes
            "LineNum": 0,  # A medida que agrega, va aumentando este número
            "ItemCode": self.get_plu(row),
            "Quantity": self.make_int(row, 'Cantidad'),  # Debe ser calculado dinámicamente.
            "BatchNumbers": [
                # Existe la posibilidad de un traslado del mismo producto de varios lotes?,
                # todos desde una misma bodega hacia otra?.
                {
                    "BatchNumber": row['Lote'],
                    "Quantity": self.make_int(row, 'Cantidad')
                }
            ],
            "StockTransferLinesBinAllocations": [
                {  # Bodega Origen
                    "BinAbsEntry": self.get_bin_abs_entry(row, 'CentroOrigen'),
                    "Quantity": self.make_int(row, 'Cantidad'),
                    "BaseLineNumber": 0,
                    "BinActionType": "batFromWarehouse",
                    "SerialAndBatchNumbersBaseLine": 0
                },
                {  # Bodega Destino
                    "BinAbsEntry": self.get_bin_abs_entry(row, 'CentroDestino'),
                    "Quantity": self.make_int(row, 'Cantidad'),
                    "BaseLineNumber": 0,
                    "BinActionType": "batToWarehouse",
                    "SerialAndBatchNumbersBaseLine": 0
                },
            ]
        }

    def build_document_lines(self, row) -> dict:
        # document_lines tiene los valores que son iguales para todos los modulos
        document_lines = {
            # "ItemCode": self.get_plu(row),
            "WarehouseCode": row.get("CECO", ''),
            "CostingCode2": row.get("CECO", ''),
        }
        match self.name:
            case settings.COMPRAS_NAME:  # 7
                for key in ('CostingCode2',):
                    document_lines.pop(key, None)
                document_lines.update(
                    ItemCode=self.get_plu(row),
                    BatchNumbers=[
                        {
                            "BatchNumber": row.get("Lote", ''),
                            "Quantity": self.make_int(row, 'Cantidad'),
                            "ExpiryDate": self.transform_date(row, 'FechaVencimiento')
                        }
                    ],
                )
                document_lines.update(Quantity=self.get_num_in_buy(row, document_lines['BatchNumbers'][0]['Quantity']))
                if not document_lines['Quantity']:
                    document_lines.update(UnitPrice=self.make_float(row, 'Precio'))
                elif document_lines['Quantity'] == 1:
                    document_lines.update(UnitPrice=self.make_float(row, 'Precio'))
                else:
                    qty_csv = document_lines['BatchNumbers'][0]['Quantity']
                    qty_embalaje_calculado = document_lines['Quantity']
                    embalaje_plu = qty_csv // qty_embalaje_calculado
                    document_lines.update(UnitPrice=self.make_float(row, 'Precio') * embalaje_plu)
            case settings.AJUSTES_ENTRADA_PRUEBA_NAME:
                document_lines.update(
                    ItemCode=self.get_plu(row, 'codigo'),
                    WarehouseCode=row.get("bodega_ent", ''),
                    CostingCode2=row.get("bodega_ent", ''),
                    Quantity=self.make_int(row, "cantidad"),
                    CostingCode=self.get_costing_code(row, "bodega_ent"),
                    AccountCode="7165950302",
                    UnitPrice=self.make_float(row, 'Costo'),
                    BatchNumbers=[
                        {
                            "BatchNumber": row["lote"],
                            "Quantity": self.make_int(row, 'cantidad'),
                            "ExpiryDate": self.transform_date_v2(row, 'fecha_venc')
                        }
                    ]
                )
            case settings.AJUSTES_ENTRADA_NAME:  # 8.2
                document_lines.update(
                    ItemCode=self.get_plu(row),
                    Quantity=self.make_int(row, "Cantidad"),
                    CostingCode=self.get_costing_code(row),
                    AccountCode=self.get_centro_de_costo(row, 'TipoAjuste', 'entrada'),
                    UnitPrice=self.make_float(row, 'Precio'),
                    BatchNumbers=[
                        {
                            "BatchNumber": row["Lote"],
                            "Quantity": self.make_int(row, 'Cantidad'),
                            "ExpiryDate": self.transform_date(row, 'FechaVencimiento')
                        }
                    ]
                )
            case settings.AJUSTES_SALIDA_NAME:  # 8.1
                document_lines.update(
                    ItemCode=self.get_plu(row),
                    Quantity=self.make_int(row, "Cantidad"),
                    CostingCode=self.get_costing_code(row),
                    AccountCode=self.get_centro_de_costo(row, 'TipoAjuste', 'salida'),
                    BatchNumbers=[
                        {
                            "BatchNumber": row["Lote"],
                            "Quantity": self.make_int(row, 'Cantidad'),
                        }
                    ]
                )
            case settings.DISPENSACION_NAME:  # 4 y 5
                if row[self.pk] in self.data and self.data[row[self.pk]]['json']:
                    self.single_serie = self.data[row[self.pk]]['json']['Series']
                if self.single_serie == self.series['CAPITA']:  # 4
                    document_lines.update(
                        ItemCode=self.get_plu(row),
                        Quantity=self.make_int(row, "CantidadDispensada"),
                        AccountCode=self.get_centro_de_costo(row, 'SubPlan'),
                        CostingCode=self.get_costing_code(row),
                        CostingCode3=self.get_contrato(row),
                        BatchNumbers=[
                            {
                                "BatchNumber": row.get("Lote", ''),
                                "Quantity": self.make_int(row, "CantidadDispensada"),
                            }
                        ]
                    )
                elif self.single_serie == self.series['EVENTO']:  # 5
                    document_lines.update(
                        # LineNum=0,  # TODO: Es el renglon en SAP del Item.
                        ItemCode=self.get_plu(row),
                        Quantity=self.make_int(row, "CantidadDispensada"),
                        Price=self.make_float(row, "Precio"),
                        CostingCode=self.get_costing_code(row),
                        CostingCode3=self.get_contrato(row),
                        BatchNumbers=[
                            {
                                "BatchNumber": row.get("Lote", ''),
                                "Quantity": self.make_int(row, "CantidadDispensada"),
                            }
                        ]
                    )
            case settings.DISPENSACIONES_ANULADAS_NAME:
                document_lines.update(
                    ItemCode=self.get_plu(row),
                    Quantity=self.make_int(row, "Cantidad"),
                    CostingCode=self.get_costing_code(row),
                    CostingCode3=self.get_contrato(row),
                    AccountCode=self.get_centro_de_costo(row, 'SubPlan'),
                    UnitPrice=self.make_float(row, 'Precio'),
                    BatchNumbers=[
                        {
                            "BatchNumber": row["Lote"],
                            "Quantity": self.make_int(row, 'Cantidad'),
                            "ExpiryDate": self.transform_date(row, 'FechaVencimiento')
                        }
                    ]
                )
            case settings.FACTURACION_NAME:  # 5.1
                document_lines.update(
                    Quantity=self.make_int(row, "CantidadDispensada"),
                    Price=self.make_float(row, "Precio"),
                    CostingCode=self.get_costing_code(row),
                    CostingCode3=self.get_contrato(row),
                )
                if document_lines.get('WarehouseCode') == '391':
                    # self.pending_to_implement(row, 'No se ha implementado facturación cuando CECO es 391')
                    document_lines.update(
                        ItemDescription=f"{row.get('Plu', '')} {row['Articulo']}".strip(),
                        ItemCode=self.get_iva_code(row)
                    )
                else:
                    document_lines.update(
                        BaseLine=0,
                        BaseType="15",
                        BaseEntry=self.get_doc_entry_factura(row),
                        ItemCode=self.get_plu(row),
                    )
            case settings.NOTAS_CREDITO_NAME:
                fecha_factura_creada = row.get('FecCreFactura')
                if not fecha_factura_creada or is_later_than_january_31_2024(string_to_datetime(fecha_factura_creada)):
                    document_lines.update(
                        BaseType="13",
                        BaseEntry=self.get_info_sap_entrega(row, 'DocEntry'),
                        BaseLine=self.get_info_sap_entrega(row, 'BaseLine'),
                        StockInmPrice=self.get_info_sap_entrega(row, 'StockPrice'),
                    )
                document_lines.update(
                    ItemCode=self.get_plu(row),
                    Quantity=self.make_int(row, "CantidadDispensada"),
                    Price=self.make_float(row, "Precio"),
                    CostingCode=self.get_costing_code(row),
                    CostingCode3=self.get_contrato(row),
                    BatchNumbers=[
                        {
                            "BatchNumber": row.get("Lote", ''),
                            "Quantity": self.make_int(row, "CantidadDispensada"),
                        }
                    ]
                )

        return document_lines

    def build_base(self, key, row):
        base_dct = {
            # "Comments": row.get("Observaciones"),
            "Comments": load_comments(row, 'NroDocumento'),  # Agregar Nro Documento en traslados, notas_credito...
            "U_LF_IdAfiliado": row.get("NroDocumento", ''),
            "U_LF_Formula": key,
            "U_LF_Mipres": row.get("Mipres", ''),
            "U_LF_Usuario": row.get("UsuarioDispensa", '')
        }
        match self.name:
            case settings.COMPRAS_NAME:  # 7
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    U_LF_NroDocumento=f"Comp{row[self.pk]}",
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaCompra'),
                    NumAtCard=row["Factura"],
                    CardCode=self.get_nit_compras(row),
                    # TODO de donde se obtiene Prefijo PN+Nit del cliente (PR90056320) debe estar creado en sap?
                    DocumentLines=[self.build_document_lines(row)],
                )
            case settings.TRASLADOS_NAME:  # 6
                for key in ('Comments', "U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    U_LF_NroDocumento=row[self.pk],
                    DocDate=self.transform_date(row, 'FechaTraslado'),
                    CardCode="PRV900073223",
                    JournalMemo=load_comments(row),
                    FromWarehouse=row['CentroOrigen'],
                    ToWarehouse=row['CentroDestino'],
                    StockTransferLines=[self.build_stock_transfer_lines(row)]
                )
            case settings.AJUSTES_ENTRADA_PRUEBA_NAME:  # 8.2
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date_v2(row, 'fecha_tras'),
                    DocDueDate=self.transform_date_v2(row, 'fecha_tras'),
                    U_HBT_Tercero="PRV900073223",
                    Comments=load_comments(row, 'usuario'),
                    DocumentLines=[self.build_document_lines(row)],
                )
            case settings.AJUSTES_ENTRADA_NAME:  # 8.2
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    U_LF_NroDocumento=f"AjEnt{row[self.pk]}",
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaAjuste'),
                    DocDueDate=self.transform_date(row, 'FechaAjuste'),
                    U_HBT_Tercero="PRV900073223",
                    DocumentLines=[self.build_document_lines(row)],
                )
            case settings.AJUSTES_SALIDA_NAME:  # 8.1
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    U_LF_NroDocumento=f"AjSal{row[self.pk]}",
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaAjuste'),
                    DocDueDate=self.transform_date(row, 'FechaAjuste'),
                    U_HBT_Tercero="PRV900073223",
                    DocumentLines=[self.build_document_lines(row)],
                )
            case settings.AJUSTES_LOTE_NAME:
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario", "Comments"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.get_abs_entry_from_lote(row),
                    ExpirationDate=self.transform_date(row, 'FechaVencimiento')
                )
            case settings.DISPENSACION_NAME:  # 4 y 5 [Implementado]
                base_dct.update(Series=self.get_series(row))
                if base_dct['Series'] == 89:  # 4
                    base_dct.update(DocDate=self.transform_date(row, "FechaDispensacion"))
                    base_dct.update(
                        U_LF_IDSSC=self.generate_idssc(row, base_dct.get('DocDate')),
                        TaxDate=self.transform_date(row, "FechaDispensacion"),
                        U_HBT_Tercero=self.get_codigo_tercero(row),
                        U_LF_Plan=self.get_plan(row),
                        U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                        U_LF_NivelAfiliado=self.make_int(row, "Categoria"),
                        U_LF_Autorizacion=self.get_num_aut(row),
                        JournalMemo="Escenario dispensación medicar",
                        DocumentLines=[self.build_document_lines(row)],
                    )
                elif base_dct['Series'] == 11:  # 5
                    base_dct.update(DocDate=self.transform_date(row, "FechaDispensacion"))
                    base_dct.update(
                        U_LF_IDSSC=self.generate_idssc(row, base_dct.get('DocDate')),
                        TaxDate=self.transform_date(row, "FechaDispensacion"),
                        CardCode=self.get_codigo_tercero(row),
                        U_HBT_Tercero=self.get_codigo_tercero(row),
                        U_LF_Plan=self.get_plan(row),
                        U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                        U_LF_NivelAfiliado=self.make_int(row, "Categoria"),
                        U_LF_Autorizacion=self.get_num_aut(row),
                        DocumentLines=[self.build_document_lines(row)],
                    )
                else:
                    # Si no fue posible definir el Series por algun motivo.
                    base_dct.update(DocDate=self.transform_date(row, "FechaDispensacion"))
                    base_dct.update(
                        U_LF_IDSSC=self.generate_idssc(row, base_dct.get('DocDate')),
                        U_HBT_Tercero=self.get_codigo_tercero(row),
                        U_LF_NivelAfiliado=self.make_int(row, "CategoriaActual"),
                        U_LF_Plan=self.get_plan(row),
                        U_LF_Autorizacion=self.get_num_aut(row),
                        TaxDate=self.transform_date(row, "FechaDispensacion"),
                        U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                        DocumentLines=[self.build_document_lines(row)],
                    )
            case settings.DISPENSACIONES_ANULADAS_NAME:  # 8.2
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaAnulacion'),
                    DocDueDate=self.transform_date(row, 'FechaAnulacion'),
                    U_LF_IdAfiliado=row.get("NroDocumentoAfiliado", ''),
                    U_LF_Usuario=row.get("UsuarioDispensa", ''),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    U_LF_Plan=self.get_plan(row),
                    U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                    U_LF_NivelAfiliado=self.make_int(row, "CategoriaActual"),
                    U_LF_Autorizacion=self.make_int(row, 'NroAutorizacion') if row["NroAutorizacion"] != '' else '',
                    DocumentLines=[self.build_document_lines(row)],
                )
            case settings.FACTURACION_NAME:  # 5.1, 2  [Implementado]
                base_dct.update(
                    Series=self.get_series(row),
                    DocDate=self.transform_date(row, "FechaFactura"),
                    TaxDate=self.transform_date(row, "FechaFactura"),
                    NumAtCard=row["Factura"],
                    CardCode=self.get_codigo_tercero(row),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    U_LF_Plan=self.get_plan(row),
                    U_LF_NivelAfiliado=self.make_int(row, "Categoria"),
                    U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                    U_LF_Autorizacion=self.get_num_aut(row),
                    DocumentLines=[self.build_document_lines(row)],
                    Comments=load_comments(row, 'Factura')
                )
                base_dct.update(WithholdingTaxDataCollection=[
                    {
                        "WTCode": "RFEV",
                        "Rate": 100,
                        "U_HBT_Retencion": (base_dct['DocumentLines'][0]['Quantity'] *
                                            base_dct['DocumentLines'][0]['Price'])
                    }
                ], )
            case settings.NOTAS_CREDITO_NAME:
                base_dct.update(
                    Series=self.get_series(row),
                    DocDate=self.transform_date(row, "FechaFactura"),
                    TaxDate=self.transform_date(row, "FechaFactura"),
                    NumAtCard=row["Factura"],
                    CardCode=self.get_codigo_tercero(row),
                    U_LF_Plan=self.get_plan(row),
                    U_LF_NivelAfiliado=self.make_int(row, "CategoriaActual"),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                    U_LF_Autorizacion=self.get_num_aut(row),
                    Comments=load_comments(row, 'UsuarioDispensa'),
                    U_LF_Mipres=row.get("MiPres", ''),
                    DocumentLines=[self.build_document_lines(row)],
                )
            case settings.PAGOS_RECIBIDOS_NAME:
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario", "Comments"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaPago'),
                    CardCode=self.get_codigo_tercero(row),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    Remarks=load_comments(row, self.pk),
                    JournalRemarks=load_comments(row, self.pk),
                    CashAccount='1105050101',  # Cada punto debe tener su cuenta
                    CashSum=self.make_float(row, 'Valor'),
                    ControlAccount='2805950101'
                )

        return base_dct

    def add_article(self, key: str, article: dict) -> None:
        """
        Dispensación:
        Busca en la lista de DocumentLines si el articulo no está,
        de ser asi agrega a la lista el diccionario article, de lo
        contrario, busca el articulo a partir del ItemCode y agrega
        la información del lote, sumando la cantidad al Quantity.
        Traslados:
        Busca en la lista de StockTransferLines si el articulo no está,
        de ser así

        :param key: Referencia del primary key. Esta referencia
                    es util para agregar el articulo entrante.
        :param article: Artículo recién leido del csv.
        :return:
        """
        match self.name:
            case 'dispensacion' | 'ajustes_entrada' | 'ajustes_salida' \
                 | 'notas_credito' | 'compras' | 'ajustes_entrada_prueba' | 'dispensaciones_anuladas':
                lst_item_codes = [code['ItemCode'] for code in self.data[key]['json']["DocumentLines"]]
                try:
                    idx = lst_item_codes.index(article['ItemCode'])
                except ValueError:
                    self.data[key]['json']["DocumentLines"].append(article)
                except TypeError:
                    ...
                else:
                    self.data[key]['json']["DocumentLines"][idx]['Quantity'] += article['Quantity']
                    self.data[key]['json']["DocumentLines"][idx]['BatchNumbers'].append(article['BatchNumbers'][0])
                    if self.name == 'compras':
                        self.data[key]['json']["DocumentLines"][idx]['UnitPrice'] *= \
                            self.data[key]['json']["DocumentLines"][idx]['Quantity']
            case settings.TRASLADOS_NAME:
                lst_item_codes = [code['ItemCode'] for code in self.data[key]['json']["StockTransferLines"]]
                try:
                    idx = lst_item_codes.index(article['ItemCode'])
                except ValueError:
                    if self.data[key]['json']["StockTransferLines"]:
                        last_line_num = self.data[key]['json']["StockTransferLines"][-1]['LineNum']
                        article.update(LineNum=last_line_num + 1)
                        article['StockTransferLinesBinAllocations'][0].update(BaseLineNumber=last_line_num + 1)
                        article['StockTransferLinesBinAllocations'][1].update(BaseLineNumber=last_line_num + 1)
                    self.data[key]['json']["StockTransferLines"].append(article)
                else:
                    self.data[key]['json']["StockTransferLines"][idx]['BatchNumbers'].extend(article['BatchNumbers'])
                    self.data[key]['json']["StockTransferLines"][idx]['Quantity'] += article['Quantity']
                    self.data[key]['json']["StockTransferLines"][idx]['StockTransferLinesBinAllocations'][0][
                        'Quantity'] += article['Quantity']
                    self.data[key]['json']["StockTransferLines"][idx]['StockTransferLinesBinAllocations'][1][
                        'Quantity'] += article['Quantity']

                    self.data[key]['json']["StockTransferLines"][idx]['StockTransferLinesBinAllocations'][0][
                        'BaseLineNumber'] = self.data[key]['json']["StockTransferLines"][idx]['LineNum']
                    self.data[key]['json']["StockTransferLines"][idx]['StockTransferLinesBinAllocations'][1][
                        'BaseLineNumber'] = self.data[key]['json']["StockTransferLines"][idx]['LineNum']
            case settings.FACTURACION_NAME:
                lst_item_codes = [code['ItemCode'] for code in self.data[key]['json']["DocumentLines"]]
                try:
                    idx = lst_item_codes.index(article['ItemCode'])
                except ValueError:
                    if article['WarehouseCode'] != '391':
                        article['BaseLine'] = self.data[key]['json']["DocumentLines"][-1]['BaseLine'] + 1
                    self.data[key]['json']["DocumentLines"].append(article)
                else:
                    self.data[key]['json']["DocumentLines"][idx]['Quantity'] += article['Quantity']
                finally:
                    self.data[key]['json']["WithholdingTaxDataCollection"][0]['U_HBT_Retencion'] += (article['Quantity']
                                                                                                     * article['Price'])

    def process_module(self, csv_reader):
        for i, row in enumerate(csv_reader, 1):
            key = row[self.pk]

            log.info(f'LN {i} Leyendo {self.pk} {key}')
            row['Status'] = ''
            if key in self.data:
                if self.name in settings.MODULES_USE_DOCUMENTLINES:
                    self.add_article(key, self.build_document_lines(row))
                    self.data[key]['csv'].append(row)
                if self.name in (settings.TRASLADOS_NAME,):
                    self.add_article(key, self.build_stock_transfer_lines(row))
                    self.data[key]['csv'].append(row)
                if self.name in (settings.PAGOS_RECIBIDOS_NAME,):
                    self.data[key]["PaymentInvoices"].append(self.build_payment_invoices(row))
                self.update_status_necessary_columns(row, key)
            elif key != '':
                # Entra aquí la primera vez que itera sobre el pk
                # log.info(f'{i} [{self.name.capitalize()}] Leyendo {self.pk} {key}')
                self.succss.add(key)
                self.data[key] = {'json': {}, 'csv': []}
                self.data[key]['json'] = self.build_base(key, row)
                self.data[key]['csv'].append(row)
            else:
                # Entra aquí cuando viene el pk vacío, entonces
                # crea un key para que sea tenido en cuenta en la exportación de csv
                new_key = f"sin {self.pk.lower()} ({i})"
                txt = f"[CSV] {self.pk} desconocido para {self.name.capitalize()}: {key!r}"
                log.info(f'{i} {txt}')
                self.data[new_key] = {'json': {}, 'csv': []}
                row['Status'] = txt
                self.data[new_key]['json'] = self.build_base(key, row)
                self.data[f"sin {self.pk.lower()} ({i})"]['csv'].append(row)
                self.reg_error(row, txt)
        log.info(f'Leidas {i} lineas del csv.')
        self.csv_lines = i
        return True

    def update_status_necessary_columns(self, row, key):
        """ Actualiza el campo de status en el resto de lineas del mismo
        documento caso esten vacías. """
        if key in self.errs:
            if not row['Status']:
                self.data[key]['csv'][-1]['Status'] = self.data[key]['csv'][-2]['Status']
            else:
                for r in self.data[row[self.pk]]['csv']:
                    r['Status'] = row['Status']
