from dataclasses import dataclass, field

from core.settings import logger as log
from utils.decorators import logtime
from utils.resources import datetime_str
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

    def __repr__(self):
        return f"Csv2Dict(name='{self.name}', " \
               f"pk='{self.pk}', series={self.series}, " \
               f"data={len(self.data.values())})"

    def reg_error(self, row, txt):
        """Agrega el motivo del error a la fila recibida"""
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
        """Determina el series a partir del SubPlan o No"""
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
            case "CAPITA":
                return "CAPSUB01"
            case "CAPITA SUBSIDIADO":
                return "CAPSUB01"
            case "CAPITA CONTRIBUTIVO":
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

    def get_centro_de_costo(self, row: dict) -> str:
        """
        Define el AccountCode a partir de un conjunto de constantes
        :param row: Diccionario con datos que vienen del csv.
        :return: Centro de costo definido por contabilidad.
        """
        match row.get('SubPlan', '').upper():
            case "CAPITA":
                return "7165950102"
            case "CAPITA SUBSIDIADO":
                return "7165950102"
            case "CAPITA CONTRIBUTIVO":
                return "7165950101"
            case "EVENTO PBS CONTRIBUTIVO":
                return "7165950202"
            case "EVENTO NO PBS SUBSIDIADO":
                return "7165950203"
            case "EVENTO NO PBS CONTRIBUTIVO":
                return "7165950204"
            case "EVENTO PBS SUBSIDIADO":
                return "7165950201"
            case "":
                return ""
            case _:
                txt = f"[CSV] SubPlan no reconocido para centro de costo {row.get('SubPlan')!r}"
                log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
                self.reg_error(row, txt)

    def get_costingcode(self, row) -> str:
        """
        Determina el CostingCode apartir del CECO.
        CECO es un número que se consulta en API de SAP
        y del resultado se toma el U_HBT_Dimension1
        :return:
        """
        try:
            ceco = row['CECO']
            costing_code = self.sap.get_costing_code_from_surcusal(ceco)
            if not costing_code:
                raise Exception()
        except Exception as e:
            txt = f"[CSV] CECO no reconocido {row['CECO']}"
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
        """
        Transforma la fecha del formato "2022-12-31 18:36:00" a "20221231"
        """
        try:
            dt = row[column_name]
            anho, mes, dia = dt.split(' ')[0].split('-')
        except Exception as e:
            log.error(f"{self.pk} {row[f'{self.pk}']}. Fecha '{dt}' no pudo ser transformada. "
                  f'Se esperaba el formato "2022-12-31 18:36:00" y se recibió {dt}')
            self.reg_error(row, f'Formato inesperado en {column_name} Esperado "2022-12-31 18:36:00"')
        else:
            return f"{anho}{mes}{dia}"

    def get_codigo_tercero(self, row: dict) -> str:
        """Determina el codigo del nit, agregándole CL al
        comienzo del valor recibido. """
        nit = row.get('NIT', '')
        if nit != '':
            return f"CL{nit}"
        log.error(f"{self.pk} {row[f'{self.pk}']}. NIT no reconocido : {nit!r}")
        self.reg_error(row, f"[CSV] NIT no reconocido: {nit!r}")

    def get_nombre_afiliado(self, row: dict) -> str:
        person = row.get("Beneficiario", '')
        if person != '':
            return person
        log.error(f"{self.pk} {row[f'{self.pk}']}. Beneficiario no reconocido : {person!r}")
        self.reg_error(row, f"[CSV] Beneficiario no reconocido: {person!r}")

    def make_float(self, row, colum_name):
        """ Intenta conviertir valor a decimal."""
        try:
            vlr = float(row[colum_name])
        except Exception as e:
            txt = f"[CSV] {colum_name} '{row[colum_name]}' " \
                  "no pudo ser convertido a decimal."
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return vlr

    def make_int(self, row, colum_name):
        """ Intenta conviertir valor a decimal."""
        try:
            num = int(row[colum_name])
        except Exception as e:
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
        if self.single_serie == self.series['EVENTO']:
            self.make_int(row, "Numero Autorizacion")
        return ''

    def get_plu(self, row):
        item_code = row.get("Plu", '')
        if item_code != '':
            return item_code
        log.error(f"{self.pk} {row[f'{self.pk}']}. Plu no reconocido : {item_code!r}")
        self.reg_error(row, f"[CSV] Plu no reconocido: {item_code!r}")
    def get_doc_entry(self, row):
        """
        Busca el doc entry (registro en sap) de la entrega correspondiente
        """
        # TODO: row['Plu'] puede llegar vacío y eso es un problema
        try:
            if dentry := self.sap.get_docentry_entrega(row[self.pk]):
                res = list(filter(lambda v: v['ItemCode'] == row['Plu'], dentry))
            else:
                raise Exception()
        except Exception as e:
            txt = f"[CSV] No se encontraron entregas para SSC {row[self.pk]}"
            log.error(f"{self.pk} {row[f'{self.pk}']}. {txt}")
            self.reg_error(row, txt)
        else:
            return res[0]['DocEntry']

    @logtime('CSV')
    def process(self, csv_reader):
        log.info("Comenzando procesamiendo de CSV.")
        self.process_module(csv_reader)
        log.info("CSV procesado con éxito.")
        log.error(f"Dispensaciones con error {len(self.errs)}: {' '.join(self.errs) if self.errs else ''}")

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
        return ...

    def build_document_lines(self, row):
        # document_lines tiene los valores que son iguales para todos los modulos
        document_lines = {
            "ItemCode": self.get_plu(row),
            "WarehouseCode": row.get("CECO", ''),
            "AccountCode": self.get_centro_de_costo(row),
            "CostingCode": self.get_costingcode(row),
            "CostingCode2": row.get("CECO", ''),
            "CostingCode3": self.get_contrato(row),
            "BatchNumbers": [
                {
                    "BatchNumber": row.get("Lote", ''),
                    "Quantity": self.make_int(row, "Cantidad Dispensada"),
                }
            ]
        }
        match self.name:
            case 'factura':  # 1 [No se usará]
                document_lines.pop('AccountCode', None)
                document_lines.update(
                    Price=self.make_float(row, "Vlr.Unitario Margen"),
                    Quantity=self.make_int(row, "Cantidad Dispensada")
                )
            case 'factura_eventos':  # 1.2
                document_lines.pop('BatchNumbers', None)
                document_lines.pop('AccountCode', None)
                document_lines.update(
                    ItemCode="",  # TODO: Preguntar a Marlay cual es el ItemCode Cuando se trata de 1.2 Factura Eventos 391
                    ItemDescription=row["Articulo"],
                    Price=self.make_float(row, "Vlr.Unitario Margen"),
                    Quantity=self.make_int(row, "Cantidad Dispensada"),
                    CostingCode3=str(),  # TODO: Preguntar a Marlay de donde se saca el # de contrato?
                )
            case 'facturacion':  # 5.1
                document_lines.pop('BatchNumbers', None)
                document_lines.pop('AccountCode', None)
                document_lines.update(
                    Quantity=self.make_int(row, "Cantidad Dispensada"),
                    Price=self.make_float(row, "Vlr.Unitario Margen"),
                    BaseType="15",
                    BaseEntry=self.get_doc_entry(row),  # TODO: DocEntry de la entrega parcial. Preguntar a Marlay como consultarlo en SAP ?
                    BaseLine=0,  # TODO: Preguntar a Marlay
                )
            case 'notas_credito':  # 2
                document_lines.pop('AccountCode', None)
                document_lines.update(
                    BaseType="13",
                    BaseEntry=int(),  # TODO: DocEntry de la factura a cancelar
                    BaseLine=0,  # TODO: Preguntar a Marlay
                    Price=self.make_float(row, "Vlr.Unitario Margen"),
                    StockInmPrice=row["Costo Unitario"],
                )
            case 'dispensacion':  # 4 y 5
                if self.single_serie == self.series['CAPITA']:  # 4
                    document_lines.update(
                        Quantity=self.make_int(row, "Cantidad Dispensada"),
                    )
                elif self.single_serie == self.series['EVENTO']:  # 5
                    document_lines.pop('AccountCode', None)
                    document_lines.update(
                        # LineNum=0,  # TODO: Es el renglon en SAP del Item.
                        Quantity=self.make_int(row, "Cantidad Dispensada"),
                        Price=self.make_float(row, "Vlr.Unitario Margen"),
                    )
                else:
                    # Entraria aqui cuando no haya sido posible definir el single_serie
                    # predeterminado que podría ser o 77 o 81
                    document_lines.update(
                        Quantity=self.make_int(row, "Cantidad Dispensada"),
                    )
            case 'entrada_compras':  # 7
                # TODO: Validar con BatchNumbers entrante si el BatchNumber existe, crear otro dict en BatchNumbers.

                for key in ('AccountCode', 'CostingCode', 'CostingCode2', 'CostingCode3'):
                    document_lines.pop(key, None)

                document_lines.update(
                    Quantity=0,  # TODO: Se va a crear? o ya Medicar lo tiene, como se llama o llamaría?
                    UnitPrice='',  # TODO: Precio SIN Iva. Se va a crear o ya viene de medicar?
                )
            case 'inventario_salida':  # 8.1
                document_lines.update(
                    AccountCode="",  # TODO: Como se llega a Cuenta Contrapartida de Inventario (Costo)?
                    Quantity=str(),  # TODO: Definir cual es el Quantity Cuando se trata de 8.1. Ajuste Inventario (salida)
                    U_HBT_CONC_SALINV="001",  # TODO: Definir como se sabe Conceptos de salida de acuerdo a lo enviado ?
                )
            case 'inventario_entrada':  # 8.2
                document_lines.update(
                    AccountCode="",  # TODO: Como se llega a Cuenta Contrapartida de Inventario (Costo)?
                    Quantity=str(),  # TODO: Definir cual es el Quantity Cuando se trata de 8.2. Ajuste Inventario (entrada)
                    UnitPrice=str(),  # TODO: Definir cual es el UnitPrice Cuando se trata de 8.2. Ajuste Inventario (entrada)
                    U_HBT_CONC_SALINV="001",  # TODO: Definir cual es ?, se llama igual cuando es entrada ?
                    BatchNumbers=[
                        {
                            "BatchNumber": row["Lote"],
                            "Quantity": int(),  # TODO: Definir cual es el Quantity Cuando se trata de 8.2. Ajuste Inventario (entrada)
                            "ExpiryDate": str()  # TODO : Definir cual es el ExpiryDate ?
                        }
                    ]
                )

        return document_lines

    def build_base(self,  key, row):
        base_dct = {"Series": self.get_series(row),
                    "CardCode": self.get_codigo_tercero(row),
                    "U_HBT_Tercero": self.get_codigo_tercero(row),
                    # "Comments": row.get("Observaciones"),
                    "Comments": f"Cargue automático {datetime_str()} UsuarioSAP: Medicar",

                    "U_LF_Plan": self.get_plan(row),
                    "U_LF_IdAfiliado": row.get("Nro Documento", ''),
                    "U_LF_NombreAfiliado": self.get_nombre_afiliado(row),
                    "U_LF_Formula": key,
                    "U_LF_NivelAfiliado": self.make_int(row, "Categoria Actual"),
                    "U_LF_Autorizacion": self.get_num_aut(row),
                    "U_LF_Mipres": row.get("Mipres", ''),
                    "U_LF_Usuario": row.get("Nombre", '')
                    }
        match self.name:
            case 'facturacion' | 'notas_credito':  # 5.1, 2
                base_dct.update(
                    DocDate=self.transform_date(row, "Fecha Factura"),
                    TaxDate=self.transform_date(row, "Fecha Factura"),
                    NumAtCard=row["Factura"],
                    DocumentLines=[self.build_document_lines(row)],
                    WithholdingTaxDataCollection=[
                        {
                            "WTCode": "RFEV",
                            "Rate": 100
                        }
                    ]
                )
            case 'factura_eventos':  # 1.2
                # TODO Preguntar a Marlay, de donde se va a tomar el CardCode y U_HBT_Tercero (código del cliente?)
                base_dct.update(
                    DocDate=self.transform_date(row, "Fecha Factura"),
                    TaxDate=self.transform_date(row, "Fecha Factura"),
                    NumAtCard=row['Factura'],
                    DocumentLines=[self.build_document_lines(row)],
                    WithholdingTaxDataCollection=[
                        {
                            "WTCode": "RFEV",
                            "Rate": 100
                        }
                    ]
                )
            case 'pago_recibido':  # 3
                # TODO Preguntar a Marlay si aqui va o no el SSC
                for key in ('CardCode', 'U_HBT_Tercero', 'Comments', "U_LF_Plan", "U_LF_IdAfiliado",
                            "U_LF_NombreAfiliado", "U_LF_Formula", "U_LF_NivelAfiliado",
                            "U_LF_Autorizacion", "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    DocDate=self.transform_date(row, "Fecha Factura"),
                    Remarks="Escenario cuota moderadora",
                    JournalRemarks="Escenario cuota moderadora",
                    CashAccount=str(),  # TODO De donde obtenemos la Cuenta de Caja del Punto de dispensación
                    CashSum=row['Vlr Pagado'],
                    PaymentInvoices=[self.build_payment_invoices(row)]
                )
            case 'dispensacion':  # 4 y 5
                if base_dct['Series'] == 77:  # 4
                    base_dct.pop('CardCode', None)
                    base_dct.update(
                        JournalMemo="Escenario dispensación medicar",
                        DocDate=self.transform_date(row, "Fecha Dispensacion"),
                        DocumentLines=[self.build_document_lines(row)],
                    )
                elif base_dct['Series'] == 81:  # 5
                    base_dct.update(
                        DocDate=self.transform_date(row, "Fecha Dispensacion"),
                        TaxDate=self.transform_date(row, "Fecha Dispensacion"),
                        DocumentLines=[self.build_document_lines(row)],
                    )
                else:
                    # Si no fue posible definir el Series por algun motivo.
                    base_dct.update(
                        DocDate=self.transform_date(row, "Fecha Dispensacion"),
                        TaxDate=self.transform_date(row, "Fecha Dispensacion"),
                        DocumentLines=[self.build_document_lines(row)],
                    )
            case 'transferencia':  # 6
                for key in ('U_HBT_Tercero', 'Comments', "U_LF_Plan", "U_LF_IdAfiliado",
                            "U_LF_NombreAfiliado", "U_LF_Formula", "U_LF_NivelAfiliado",
                            "U_LF_Autorizacion", "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    DocDate=str(),  # TODO Cual es el DocDate en 6. Transferencia entre puntos
                    CardCode=str(),  # TODO de donde se obtiene el código SN de Logifarma?
                    JournalMemo="Escenario transferencia entre almacenes",
                    FromWarehouse=row['Bodega Origen'],
                    ToWarehouse=row['Bodega Destino'],
                    StockTransferLines=[self.build_stock_transfer_lines(row)]
                )
            case 'entrada_compras':  # 7
                for key in ('U_HBT_Tercero', "U_LF_Plan", "U_LF_IdAfiliado",
                            "U_LF_NombreAfiliado", "U_LF_Formula", "U_LF_NivelAfiliado",
                            "U_LF_Autorizacion", "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key)
                base_dct.update(
                    DocDate=str(),  # TODO Cual es el DocDate en 7. Entrada por Compras
                    NumAtCard=row["Factura"],
                    CardCode=str(),  # TODO de donde se obtiene Prefijo PN+Nit del cliente (PR90056320) debe estar creado en sap?
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'inventario_salida':  # 8.1
                for key in ('CardCode', "U_LF_Plan", "U_LF_IdAfiliado",
                            "U_LF_NombreAfiliado", "U_LF_Formula", "U_LF_NivelAfiliado",
                            "U_LF_Autorizacion", "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    DocDate=str(),  # TODO Cual es el DocDate en 8.1. Ajuste Inventario (Salida)
                    DocDueDate=str(),  # TODO Cual es el DocDueDate en 8.1. Ajuste Inventario (Salida)
                    DocumentLines=[self.build_document_lines(row)],
                )
            case 'inventario_entrada':  # 8.2
                for key in ('CardCode', "U_LF_Plan", "U_LF_IdAfiliado",
                            "U_LF_NombreAfiliado", "U_LF_Formula", "U_LF_NivelAfiliado",
                            "U_LF_Autorizacion", "U_LF_Mipres", "U_LF_Usuario"):
                    base_dct.pop(key, None)
                base_dct.update(
                    DocDate=str(),  # TODO Cual es el DocDate en 8.2. Ajuste Inventario (Entrada)
                    DocDueDate=str(),  # TODO Cual es el DocDueDate en 8.2. Ajuste Inventario (Entrada)
                    DocumentLines=[self.build_document_lines(row)],
                )
        return base_dct

    def add_article(self, key: list, article: dict):
        """
        Busca en la lista de DocumentLines si el articulo no está,
        de ser asi agrega a la lista el diccionario article, de lo
        contrario, busca el articulo a partir del ItemCode y agrega
        la información del lote, sumando la cantidad al Quantity.
        :param key: Referencia del primary key. Esta referencia
                    es util para agregar el articulo entrante.
        :param article: Artículo recién leido del csv.
        :return:
        """
        match self.name:
            case 'dispensacion':
                lst_item_codes = [code['ItemCode'] for code in self.data[key]['json']["DocumentLines"]]
                try:
                    idx = lst_item_codes.index(article['ItemCode'])
                except ValueError:
                    self.data[key]['json']["DocumentLines"].append(article)
                else:
                    self.data[key]['json']["DocumentLines"][idx]['Quantity'] += article['Quantity']
                    self.data[key]['json']["DocumentLines"][idx]['BatchNumbers'].append(article['BatchNumbers'][0])

    def process_module(self, csv_reader):
        for i, row in enumerate(csv_reader, 1):
            key = row[self.pk]
            log.info(f'{i} [{self.name.capitalize()}] Leyendo {self.pk} {key}')
            row['Status'] = ''
            if key in self.data:
                if self.name in ('factura', 'facturacion', 'notas_credito', 'dispensacion', 'entregas_parciales', 'entrada_compras', 'inventario_entrada', 'inventario_salida'):
                    self.add_article(key, self.build_document_lines(row))
                    self.data[key]['csv'].append(row)
                if self.name in ('transferencia', ):
                    self.data[key]["StockTransferLines"].append(self.build_stock_transfer_lines(row))
                if self.name in ('pago_recibido'):
                    self.data[key]["PaymentInvoices"].append(self.build_payment_invoices(row))
            elif key != '':
                # log.info(f'{i} [{self.name.capitalize()}] Leyendo {self.pk} {key}')
                self.succss.add(key)
                self.data[key] = {'json': {}, 'csv': []}
                self.data[key]['json'] = self.build_base(key, row)
                self.data[key]['csv'].append(row)
            else:
                # En caso de venir el pk vacío , crea un key para que sea tenido en cuenta en la exportación en csv
                new_key = f"sin {self.pk.lower()} ({i})"
                txt = f"[CSV] {self.pk} desconocido para {self.name.capitalize()}: {key!r}"
                log.info(f'{i} {txt}')
                self.data[new_key] = {'json': {}, 'csv': []}
                row['Status'] = txt
                self.data[new_key]['json'] = self.build_base(key, row)
                self.data[f"sin {self.pk.lower()} ({i})"]['csv'].append(row)
                self.reg_error(row, txt)
            log.info(f'Leidas {i} lineas del csv. Creados {len(self.succss)} jsons')
        return True
