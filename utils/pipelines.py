import csv
import json
import pickle
from dataclasses import dataclass

from core.settings import DISPENSACION_HEADER, FACTURACION_HEADER, NOTAS_CREDITO_HEADER, AJUSTES_ENTRADA_HEADER, \
    AJUSTES_SALIDA_HEADER, TRASLADOS_HEADER, logger as log, DISPENSACIONES_ANULADAS_HEADER, COMPRAS_HEADER, \
    AJUSTE_LOTE_HEADER, PAGOS_RECIBIDOS_HEADER, BASE_DIR
from utils.converters import Csv2Dict
from utils.gdrive.handler_api import GDriveHandler
from utils.resources import set_filename, Email


class Validate:

    def __str__(self):
        return "Validate"

    def run(self, **kwargs):
        self.validate_header(kwargs['parser'].module.name, kwargs['reader'].fieldnames)

    def validate_header(self, module_name, fieldnames):
        """Valida que os campos do modulo estejam como é esperado"""
        match module_name:
            case 'dispensacion':
                fields = DISPENSACION_HEADER
            case 'facturacion':
                fields = FACTURACION_HEADER
            case 'notas_credito':
                fields = NOTAS_CREDITO_HEADER
            case 'ajustes_entrada':
                fields = AJUSTES_ENTRADA_HEADER
            case 'dispensaciones_anuladas':
                fields = DISPENSACIONES_ANULADAS_HEADER
            case 'ajustes_salida':
                fields = AJUSTES_SALIDA_HEADER
            case 'traslados':
                fields = TRASLADOS_HEADER
            case 'compras':
                fields = COMPRAS_HEADER
            case 'ajustes_vencimiento_lote':
                fields = AJUSTE_LOTE_HEADER
            case 'pagos_recibidos':
                fields = PAGOS_RECIBIDOS_HEADER
            case _:
                fields = {}

        if diff := fields.difference(set(fieldnames)):
            # TODO enviar correo cuando esté en producción
            raise Exception(f"Hacen falta los siguientes campos: {', '.join(diff)}")


class ProcessCSV:

    def __str__(self):
        return "ProcessCSV"

    def run(self, **kwargs):
        # csv_to_dict.process(csv_reader)
        kwargs['csv_to_dict'].process(kwargs['reader'])


class ProcessSAP:

    def __str__(self):
        return "ProcessSAP"

    def run(self, **kwargs):
        """Ejecuta SAPConnect.process()"""
        kwargs['sap'].process(kwargs['csv_to_dict'])


class Export:
    # Estas variables guardan las rutas donde están los archivos.
    json_file = None
    file_processed = None
    file_errors = None
    pkl_data = None
    pkl_module = None

    @classmethod
    def class_variables(cls):
        return [file for file in (cls.json_file, cls.file_processed, cls.file_errors, cls.pkl_data, cls.pkl_module) if
                file]

    def run(self, **kwargs):
        """
        Exporta csv y/o json.
        - Caso sea local la ejecución, solamente exporta y no hace más nada.
        - Caso NO sea local la ejecución, exporta local y además hace lo siguiente:
            - Crea archivo en Drive con todos los errores.
            - Crea archivo en Drive con todos los procesados.
        Obs.: Exportar archivos locales es obligatório cuando se lee del drive.
        """
        self.local_export(**kwargs)
        if isinstance(kwargs['parser'].input, GDriveHandler):

            # Crea archivo en Drive con todos los errores
            if kwargs['csv_to_dict'].errs:
                kwargs['parser'].input.prepare_and_send_csv(
                    self.file_errors,
                    set_filename(kwargs['file']['name'], reason='errores'),
                    f"{kwargs['name_folder']}_Error"
                )

            # Crea archivo en Drive con todos los procesados
            kwargs['parser'].input.prepare_and_send_csv(
                self.file_processed,
                set_filename(kwargs['file']['name'], reason='procesados'),
                f"{kwargs['name_folder']}_Procesado"
            )

            # Mueve archivo a carpeta
            kwargs['parser'].input.move_file(kwargs['file'], f"{kwargs['name_folder']}_BackUp")

    def local_export(self, **kwargs):
        if kwargs['parser'].export:
            fp = File(kwargs['csv_to_dict'], kwargs['parser'].module.name)
            Export.file_errors = fp.make_csv(f"{kwargs['parser'].output_filepath}_only_errors.csv", only_error=True)
            Export.file_processed = fp.make_csv(f"{kwargs['parser'].output_filepath}_processed_all.csv")
            # Archivo .json es muy pesado y el e-mail no es enviado por causa de esto
            # Export.json_file = fp.make_json(f"{kwargs['parser'].output_filepath}.json")
            Export.pkl_module = fp.make_pkl(kwargs['parser'].module,
                                            filename=f"{kwargs['parser'].module.name}_module.pkl")
            Export.pkl_data = fp.make_pkl(kwargs['csv_to_dict'], filename=f"{kwargs['parser'].module.name}_data.pkl")



class Mail:

    def __str__(self):
        return "Mail"

    def run(self, **kwargs):
        """
        Envia correo definiendo el contexto y los adjuntos.
        """
        module = kwargs['parser'].module
        data = kwargs['csv_to_dict']
        module.filepath = module.filepath or kwargs['file']['name']  # Nombre del archivo en local o del drive
        # Accessa a clase Export para traer paths de archivos exportados
        attachs = kwargs['parser'].pipeline[-2].class_variables()

        # Se definen los archivos adjuntos al correo.
        # En caso no se deseen todos los mencionados en class_variables()
        # se pueden filtrar aqui.
        e = Email(module, data, attachs)

        e.send()


@dataclass
class File:
    source: Csv2Dict
    module_name: str

    def make_pkl(self, obj, filename):
        """Crea un archivo pkl de los objetos Module y CsvtoDict"""
        filepath = ''
        with open(BASE_DIR / filename, 'wb') as fp:
            pickle.dump(obj, fp)
            filepath = fp.name
            log.info(f"Archivo creado -> {filepath}")
        return filepath

    def make_json(self, jsonfilepath=None) -> None:
        # TODO Se puede dumpar datos de la llave json o csv
        try:
            if not jsonfilepath:
                jsonfilepath = f"{self.module_name}.json"
            with open(jsonfilepath, 'w', encoding='utf-8-sig') as jsonf:
                jsonf.write(json.dumps(self.source.data, indent=4, ensure_ascii=False))
        except Exception as e:
            log.error(f"Creando json con datos: {e}")
        else:
            log.info(f"Archivo creado -> {jsonfilepath}")
            return jsonfilepath

    def make_csv(self, csvfilepath: str, only_error=False, only_success=False) -> str:
        log.info(f'Creando csv {only_error=}, {only_success=}')
        rows = self.filter_csv_status(only_error, only_success)
        try:
            if rows:
                fieldnames = rows[0].keys()
                with open(csvfilepath, 'w', encoding='utf-8-sig', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
                    writer.writeheader()
                    writer.writerows(rows)
                log.info(f"Archivo creado -> {csvfilepath}")
                return csvfilepath
            else:
                log.info("No fue creado CSV localmente porque no hubo información a procesar.")
        except Exception as e:
            log.error(f"Creando csv para {self.module_name.capitalize()} al ser procesado: {e}")
        return ''

    def filter_csv_status(self, only_error, only_success) -> list:
        """
        Filtra el objeto self.source.data com base en si es deseado
        o solo los errores o solo los succes o todos.
        En ningun caso only_error y only_success deberán ser True
        al mismo tiempo.
        :param only_error: True or False
        :param only_success: True or False
        :return: Lista de dicts o luego de haber sido filtrada.
        """
        rows = []
        if only_error:
            for k, v in self.source.data.items():
                if k in self.source.errs or 'sin' in k:
                    rows.extend(v['csv'])
        elif only_success:
            for k in self.source.succss:
                rows.extend(self.source.data[k])
        else:
            for k, v in self.source.data.items():
                rows.extend(v['csv'])
        return rows

    def filter_json_status(self, only_error=False, only_success=False) -> dict:
        """
        Filtra el objeto self.source.data com base en si es deseado
        o solo los errores o solo los succes o todos.
        En ningun caso only_error y only_success deberán ser True
        al mismo tiempo.
        :param only_error: True or False
        :param only_success: True or False
        :return: Lista de dicts o luego de haber sido filtrada.
        """
        ...

