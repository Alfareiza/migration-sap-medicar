from dataclasses import dataclass, field

from base.templatetags.filter_extras import make_text_status
from core.settings import logger as log
from utils.decorators import logtime
from utils.resources import load_comments
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
        return f"Csv2Dict(name='{self.name}', " \
               f"pk='{self.pk}', series={self.series}, " \
               f"data={len(self.data.values())})"

    def group_by_type_of_errors(self):
        """
        Crea los atributos csv_errs y sap_errs donde se guardan las
        referencias de los registros que tienen ese tipo de error.
        Los cuáles podrán ser indexados en self.data.
        """
        self.result_succss, self.csv_errs, self.sap_errs = {}, {}, {}
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

    def get_series(self, row):
        """Determina el series a partir del SubPlan o No."""
        # TODO En el caso de facturación viene 'Capita complementaria Subsidiado '
        #  en la columna de subplan y por ende entra en el primer if y quiebra el
        #  código porque para facturación 'CAPITA' no existe en el dict self.series
        subplan = row.get('SubPlan', '').upper()
        if 'CAPITA' in subplan:
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
        match row.get('SubPlan', '').upper():
            case "CAPITA" | "CAPITA NUEVA EPS DISFARMA" | "CAPITA COMPLEMENTARIA SUBSIDIADO":
                return "CAPSUB01"
            case "CAPITA SUBSIDIADO":
                return "CAPSUB01"
            case "CAPITA CONTRIBUTIVO" | "CAPITA COMPLEMENTARIA CONTRIBUTIVO":
                return "CAPCON01"
            case "EVENTO PBS CONTRIBUTIVO":
                return "EVPBSCON"
            case "EVENTO NO PBS SUBSIDIADO":
                return "EVNOPBSS"
            case "EVENTO NO PBS CONTRIBUTIVO":
                return "EVPBSCON"
            case "EVENTO PBS SUBSIDIADO":
                return "EVPBSSUB"
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
        match row.get(column_name, '').upper():
            case "CAPITA" | "CAPITA SUBSIDIADO" | "CAPITA NUEVA EPS DISFARMA" | "CAPITA COMPLEMENTARIA SUBSIDIADO":
                return "7165950102"
            case "CAPITA CONTRIBUTIVO" | "CAPITA COMPLEMENTARIA CONTRIBUTIVO":
                return "7165950101"
            case "EVENTO PBS CONTRIBUTIVO":
                return "7165950202"
            case "EVENTO NO PBS SUBSIDIADO":
                return "7165950203"
            case "EVENTO NO PBS CONTRIBUTIVO":
                return "7165950204"
            case "EVENTO PBS SUBSIDIADO":
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
            case "SALIDA POR DONACION":
                return "7165950303"
            case "VENCIDOS":
                return "7165950101"
            case "":
                return ""
            case _:
                txt = f"[CSV] {column_name} no reconocido para centro de costo {row.get(column_name)!r}"
                log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
                self.reg_error(row, txt)

    def get_costing_code(self, row) -> str:
        """
        Determina el CostingCode apartir del CECO.
        CECO es un número que se consulta en API de SAP
        y del resultado se toma el U_HBT_Dimension1
        :return:
        """
        try:
            ceco = row['CECO']
            costing_code = self.sap.get_costing_code_from_sucursal(ceco)
            if not costing_code:
                raise Exception()
        except Exception:
            txt = f"[CSV] CECO no reconocido {row['CECO']!r}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return costing_code

    def get_plan(self, row: dict) -> str:
        """Determina si es subsidiado o contributivo"""
        plan = row.get('Plan', '').upper()
        if 'SUBSIDIADO' in plan or 'Capita' in plan:
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

    def transform_date(self, row: dict, column_name: str) -> str:
        """ Transforma la fecha del formato "2022-12-31 18:36:00" a "20221231" """
        try:
            dt = row[column_name]
            anho, mes, dia = dt.split(' ')[0].split('-')
        except Exception:
            log.error(f"{self.pk} {row[f'{self.pk}']}. Fecha '{dt}' no pudo ser transformada. "
                      f'Se esperaba el formato "2022-12-31 18:36:00" y se recibió {dt}')
            self.reg_error(row, f'[CSV] Formato inesperado en {column_name} se espera este formato -> 2022-12-31 18:36:00')
        else:
            return f"{anho}{mes}{dia}"

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

    def make_float(self, row, colum_name):
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

    def make_int(self, row, colum_name):
        """ Intenta conviertir valor a decimal."""
        try:
            num = int(float(row[colum_name]))  # En dispensacion este valor llega así: '9.'0
        except Exception:
            txt = f"[CSV] {colum_name} '{row[colum_name]}' " \
                  "no pudo ser convertido a numero entero."
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
        if self.single_serie == self.series.get('EVENTO'):
            return self.make_int(row, "NroAutorizacion")
        return ''

    def get_plu(self, row):
        item_code = row.get("Plu", '')
        if item_code != '':
            return item_code
        log.error(f"{self.pk} {row[f'{self.pk}']}. Plu no reconocido : {item_code!r}")
        self.reg_error(row, f"[CSV] Plu no reconocido: {item_code!r}")

    def get_doc_entry_factura(self, row):
        """Busca el doc entry de la factura correspondiente."""
        try:
            if dentry := self.sap.get_docentry_factura(row[self.pk]):
                res = list(filter(lambda v: v['ItemCode'] == row['Plu'], dentry))
            else:
                raise Exception()
        except Exception:
            txt = f"[CSV] No se encontraron facturas para SSC {row[self.pk]}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return res[0]['DocEntry']

    def get_info_sap_entrega(self, row, to_reach):
        """
        Busca el doc entry (registro en sap) de la entrega correspondiente.
        filtrando exactamente con el ItemCode del row
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
            if binentry := self.sap.get_bin_abs_entry_from_ceco(row[colum_name]):
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
                    raise Exception(f"Plu {row['Plu']} presenta incosistencia con cantidad {qty} siendo {res_api} su embalaje")
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
        log.info("Comenzando procesamiendo de CSV.")
        self.process_module(csv_reader)
        log.info("CSV procesado con éxito.")
        log.info(f"{len(self.succss)} Payload(s) creados!!.")
        log.error(f"CSV {self.name} con error {len(self.errs)}: {' '.join(self.errs) if self.errs else ''}")

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
                }
            ]
        }

    def build_document_lines(self, row):
        # document_lines tiene los valores que son iguales para todos los modulos
        document_lines = {
            "ItemCode": self.get_plu(row),
            "WarehouseCode": row.get("CECO", ''),
            "CostingCode2": row.get("CECO", ''),
        }
        match self.name:
            case 'factura_eventos':  # 1.2
                document_lines.update(
                    ItemCode="",
                    # TODO: Preguntar a Marlay cual es el ItemCode Cuando se trata de 1.2 Factura Eventos 391
                    ItemDescription=row["Articulo"],
                    Price=self.make_float(row, "Vlr.Unitario Margen"),
                    Quantity=self.make_int(row, "Cantidad"),
                    CostingCode=self.get_costing_code(row),
                    CostingCode3=str(),  # TODO: Preguntar a Marlay de donde se saca el # de contrato?
                )
            case 'facturacion':  # 5.1
                document_lines.update(
                    Quantity=self.make_int(row, "CantidadDispensada"),
                    Price=self.make_float(row, "Precio"),
                    BaseType="15",
                    BaseEntry=self.get_info_sap_entrega(row, 'DocEntry'),
                    BaseLine=0,  # TODO: Preguntar a Marlay?, o probar con varios Articles
                    CostingCode=self.get_costing_code(row),
                    CostingCode3=self.get_contrato(row),
                )
            case 'notas_credito':  # 2
                document_lines.update(
                    Quantity=self.make_int(row, "CantidadDispensada"),
                    BaseType="13",
                    BaseEntry=self.get_info_sap_entrega(row, 'DocEntry'),
                    BaseLine=self.get_info_sap_entrega(row, 'BaseLine'),
                    Price=self.make_float(row, "Precio"),
                    StockInmPrice=self.get_info_sap_entrega(row, 'StockPrice'),
                    CostingCode=self.get_costing_code(row),
                    CostingCode3=self.get_contrato(row),
                    BatchNumbers=[
                        {
                            "BatchNumber": row.get("Lote", ''),
                            "Quantity": self.make_int(row, "CantidadDispensada"),
                        }
                    ]
                )
            case 'dispensacion':  # 4 y 5
                if self.single_serie == self.series['CAPITA']:  # 4
                    document_lines.update(
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
                else:
                    # Entraria aqui cuando no haya sido posible definir el single_serie
                    # predeterminado que podría ser o 77 o 81
                    document_lines.update(
                        AccountCode=self.get_centro_de_costo(row, 'SubPlan'),
                        Quantity=self.make_int(row, "CantidadDispensada"),
                        CostingCode=self.get_costing_code(row),
                        CostingCode3=self.get_contrato(row),
                        BatchNumbers=[
                            {
                                "BatchNumber": row.get("Lote", ''),
                                "Quantity": self.make_int(row, "CantidadDispensada"),
                            }
                        ]
                    )
            case 'compras':  # 7
                for key in ('CostingCode2',):
                    document_lines.pop(key, None)
                document_lines.update(
                    UnitPrice=self.make_float(row, 'Precio'),
                    BatchNumbers=[
                        {
                            "BatchNumber": row.get("Lote", ''),
                            "Quantity": self.make_int(row, 'Cantidad'),
                            "ExpiryDate": self.transform_date(row, 'FechaVencimiento')
                        }
                    ],
                )
                document_lines.update(Quantity=self.get_num_in_buy(row, document_lines['BatchNumbers'][0]['Quantity']))
            case 'ajustes_salida':  # 8.1
                document_lines.update(
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
            case 'ajustes_entrada':  # 8.2
                document_lines.update(
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
            case 'dispensaciones_anuladas':
                document_lines.update(
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

        return document_lines

    def build_base(self, key, row):
        base_dct = {
            # "Comments": row.get("Observaciones"),
            "Comments": load_comments(row, 'NroDocumento'),  # Agregar Nro Documento en traslados, notas_credito...
            "U_LF_IdAfiliado": row.get("Nro Documento", ''),
            "U_LF_Formula": key,
            "U_LF_Mipres": row.get("Mipres", ''),
            "U_LF_Usuario": row.get("UsuarioDispensa", '')
        }
        match self.name:
            case 'facturacion':  # 5.1, 2  [Implementado]
                base_dct.update(
                    Series=self.get_series(row),
                    DocDate=self.transform_date(row, "FechaFactura"),
                    TaxDate=self.transform_date(row, "FechaFactura"),
                    NumAtCard=row["Factura"],
                    CardCode=self.get_codigo_tercero(row),
                    U_LF_IdAfiliado=row.get("Nro Documento", ''),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    U_LF_Plan=self.get_plan(row),
                    U_LF_NivelAfiliado=self.make_int(row, "Categoria"),
                    U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                    U_LF_Autorizacion=self.get_num_aut(row),
                    DocumentLines=[self.build_document_lines(row)],
                    WithholdingTaxDataCollection=[
                        {
                            "WTCode": "RFEV",
                            "Rate": 100
                        }
                    ],
                    Comments=load_comments(row, 'Factura')
                )
            case 'notas_credito':
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, "FechaFactura"),
                    TaxDate=self.transform_date(row, "FechaFactura"),
                    NumAtCard=row["Factura"],
                    CardCode=self.get_codigo_tercero(row),
                    U_LF_Plan=self.get_plan(row),
                    U_LF_NivelAfiliado=self.make_int(row, "CategoriaActual"),
                    U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                    U_LF_Autorizacion=self.make_int(row, "NroAutorizacion"),
                    Comments=load_comments(row, 'UsuarioDispensa'),
                    U_LF_Mipres=row.get("MiPres", ''),
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'factura_eventos':  # 1.2  [Pendiente en probar con csv (puede ser que se mezcle con el de arriba)]
                # TODO Preguntar a Marlay, de donde se va a tomar el CardCode y U_HBT_Tercero (código del cliente?)
                base_dct.update(
                    Series=self.get_series(row),
                    DocDate=self.transform_date(row, "Fecha Factura"),
                    TaxDate=self.transform_date(row, "Fecha Factura"),
                    NumAtCard=row['Factura'],
                    CardCode=self.get_codigo_tercero(row),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    U_LF_Plan=self.get_plan(row),
                    U_LF_NivelAfiliado=self.make_int(row, "CategoriaActual"),
                    U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                    U_LF_Autorizacion=self.get_num_aut(row),
                    DocumentLines=[self.build_document_lines(row)],
                    WithholdingTaxDataCollection=[
                        {
                            "WTCode": "RFEV",
                            "Rate": 100
                        }
                    ]
                )
            case 'dispensacion':  # 4 y 5 [Implementado]
                base_dct.update(Series=self.get_series(row))
                if base_dct['Series'] == 77:  # 4
                    base_dct.update(
                        DocDate=self.transform_date(row, "FechaDispensacion"),
                        TaxDate=self.transform_date(row, "FechaDispensacion"),
                        U_HBT_Tercero=self.get_codigo_tercero(row),
                        U_LF_Plan=self.get_plan(row),
                        U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                        U_LF_NivelAfiliado=self.make_int(row, "Categoria"),
                        U_LF_Autorizacion=self.get_num_aut(row),
                        JournalMemo="Escenario dispensación medicar",
                        DocumentLines=[self.build_document_lines(row)],
                    )
                elif base_dct['Series'] == 81:  # 5
                    base_dct.update(
                        DocDate=self.transform_date(row, "FechaDispensacion"),
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
                    base_dct.update(
                        U_HBT_Tercero=self.get_codigo_tercero(row),
                        U_LF_NivelAfiliado=self.make_int(row, "CategoriaActual"),
                        U_LF_Plan=self.get_plan(row),
                        U_LF_Autorizacion=self.get_num_aut(row),
                        DocDate=self.transform_date(row, "FechaDispensacion"),
                        TaxDate=self.transform_date(row, "FechaDispensacion"),
                        U_LF_NombreAfiliado=self.get_nombre_afiliado(row),
                        DocumentLines=[self.build_document_lines(row)],
                    )
            case 'traslados':  # 6
                for key in ('Comments', "U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    DocDate=self.transform_date(row, 'FechaTraslado'),
                    CardCode="PR900073223",
                    JournalMemo=load_comments(row),
                    FromWarehouse=row['CentroOrigen'],
                    ToWarehouse=row['CentroDestino'],
                    StockTransferLines=[self.build_stock_transfer_lines(row)]
                )
            case 'compras':  # 7
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaCompra'),
                    NumAtCard=row["Factura"],
                    CardCode=self.get_nit_compras(row),
                    # TODO de donde se obtiene Prefijo PN+Nit del cliente (PR90056320) debe estar creado en sap?
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'ajustes_salida':  # 8.1
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaAjuste'),
                    DocDueDate=self.transform_date(row, 'FechaAjuste'),
                    U_HBT_Tercero="PR900073223",
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'ajustes_entrada':  # 8.2
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaAjuste'),
                    DocDueDate=self.transform_date(row, 'FechaAjuste'),
                    U_HBT_Tercero="PR900073223",
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'dispensaciones_anuladas':  # 8.2
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
                    U_LF_Autorizacion=self.make_int(row, 'NroAutorizacion'),
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'ajustes_vencimiento_lote':
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario", "Comments"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.get_abs_entry_from_lote(row),
                    ExpirationDate=self.transform_date(row, 'FechaVencimiento')
                )
            case 'pagos_recibidos':
                for key in ("U_LF_IdAfiliado", "U_LF_Formula",
                            "U_LF_Mipres", "U_LF_Usuario", "Comments"):
                    base_dct.pop(key, None)
                base_dct.update(
                    Series=self.series,
                    DocDate=self.transform_date(row, 'FechaPago'),
                    CardCode=self.get_codigo_tercero(row),
                    U_HBT_Tercero=self.get_codigo_tercero(row),
                    Remarks=load_comments(row, 'NroDocumento'),
                    JournalRemarks=load_comments(row, 'NroDocumento'),
                    CashAccount='1105050103',  # Cada punto debe tener su cuenta
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
            case 'dispensacion' | 'ajustes_entrada' | 'ajustes_salida' | 'notas_credito' | 'compras':
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
            case 'traslados':
                lst_item_codes = [code['ItemCode'] for code in self.data[key]['json']["StockTransferLines"]]
                try:
                    idx = lst_item_codes.index(article['ItemCode'])
                except ValueError:
                    if self.data[key]['json']["StockTransferLines"]:
                        last_line_num = self.data[key]['json']["StockTransferLines"][-1]['LineNum']
                        article.update(LineNum=last_line_num + 1)
                    self.data[key]['json']["StockTransferLines"].append(article)
                else:
                    self.data[key]['json']["StockTransferLines"][idx]['BatchNumbers'].extend(article['BatchNumbers'])
                    self.data[key]['json']["StockTransferLines"][idx]['Quantity'] += article['Quantity']
                    self.data[key]['json']["StockTransferLines"][idx]['StockTransferLinesBinAllocations'][0][
                        'Quantity'] += article['Quantity']
                    self.data[key]['json']["StockTransferLines"][idx]['StockTransferLinesBinAllocations'][1][
                        'Quantity'] += article['Quantity']
            case 'facturacion':
                self.data[key]['json']["DocumentLines"].append(article)

    def process_module(self, csv_reader):
        for i, row in enumerate(csv_reader, 1):
            key = row[self.pk]

            log.info(f'{i} [{self.name.capitalize()}] Leyendo {self.pk} {key}')
            row['Status'] = ''
            if key in self.data:
                if self.name in ('facturacion', 'notas_credito', 'dispensacion', 'compras', 'ajustes_entrada', 'ajustes_salida'):
                    self.add_article(key, self.build_document_lines(row))
                    self.data[key]['csv'].append(row)
                if self.name in ('traslados',):
                    self.add_article(key, self.build_stock_transfer_lines(row))
                    self.data[key]['csv'].append(row)
                if self.name in ('pagos_recibidos', ):
                    self.data[key]["PaymentInvoices"].append(self.build_payment_invoices(row))
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
