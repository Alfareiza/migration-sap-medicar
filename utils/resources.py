from datetime import datetime
from pathlib import Path
from smtplib import SMTPSenderRefused
from typing import List

from decouple import config, Csv
from django.core.mail import EmailMessage
from django.template.loader import get_template
from django.utils.safestring import SafeString
from pytz import timezone

from core import settings
from core.settings import BASE_DIR, logger


def moment():
    return datetime.now(tz=timezone('America/Bogota'))


def set_filename_now(filename, extra_text='') -> str:
    """
    Dado un nombre de archivo le agrega una 'razón' al nombre del mismo.
    :param filename: Ex.: 'myfile.csv'
    :param extra_text: Ex.: 'procesados'
    :return: 'myfile_procesados.csv'
    """
    # En caso el archivo tenga varios puntos a lo largo del nombre
    # el programa solo le remplazará el primero por un espacio vacío
    if filename.count('.') > 1:
        filename = filename.replace('.', '', 1)
    name, ext = filename.rsplit('.')
    if extra_text:
        name += f"_{extra_text}"
        # {format(moment, '_%H_%M_%S')}
    return f"{name}.{ext}"


def datetime_str(dt=None):
    """ Transforma la fecha en un formato específico. """
    if not dt:
        dt = moment()
    return format(dt, '%G-%m-%d %H:%M:%S')


def set_filename(name: str, reason: str) -> str:
    """
    Dado un nombre de archivo le agrega una 'razón'
    y un registro de hora, dia, segundos
    :param name: Ex.: 'myfile.csv'
    :param reason: 'errores'
    :return: 'myfile_errores.csv'
    """
    return set_filename_now(name,
                            extra_text=f"{reason}_{format(moment(), '_%H_%M_%S')}"
                            )


def clean_text(text) -> str:
    """
    Remove accents and colons from the frase
    :param text: "vÃ¡lida"
    :return: "valida"
    """
    return text.replace(',', '.')


def load_comments(row, column_name=None) -> str:
    txt = f"Cargue automático {datetime_str()} UsuarioSAP: Medicar"
    if column_name:
        extra = row.get(column_name)
        txt += f" ({column_name}. {extra})"
    return txt


def beautify_name(name):
    new_name = name.split('_')
    return ' '.join(new_name).title()


class Email:
    def __init__(self, module, data, attachs=None):
        self.module = module
        self.template = BASE_DIR / "base/templates/notifiers/index.html"
        self.email = None
        self.info_email = data
        self.set_subject()
        self.set_destinatary()
        self.set_copia_oculta()
        self.attachs: List[str] = attachs
        self.html_content = None
        self.make_html_content()

    def prepare_email(self) -> EmailMessage:
        """
        Crea clase EmailMessage basado en atributos de la instancia y adjunta
        los archivos que tenga el atributo self.attachs
        :return: Instancia de clase lista para que sea enviado el e-mail.
        """
        self.email = EmailMessage(
            self.subject, self.html_content, to=self.destinatary, bcc=self.copia_oculta,
            from_email=f"Logs de Migración <{settings.EMAIL_HOST_USER}>"
        )
        self.email.content_subtype = "html"
        size = 0
        for attachment in self.attachs:
            size += Path(attachment).stat().st_size
            if size <= 25_000_000:
                self.attach_file(attachment)

    def send(self):
        """
        from django.core.mail import EmailMessage

        email = EmailMessage( "Hello", "Body goes here", "alfareiza@gmail.com", ["alfareiza@gmail.com"])
        """
        self.prepare_email()
        try:
            if r := self.email.send(fail_silently=False):
                logger.info('E-mail enviado.')
            else:
                logger.warning('E-mail no enviado.')
        except SMTPSenderRefused as e:
            breakpoint()
            logger.warning(f'E-mail no enviado porque {e}')
        except Exception as e:
            logger.warning(f'E-mail no enviado. Error={e}')

    def attach_file(self, filepath):
        self.email.attach_file(str(filepath))

    def make_html_content(self):
        self.info_email.group_by_type_of_errors()
        htmly = get_template(self.template)  # Create a <django.template.backends.django.Template> Object
        ctx = {
            "fecha": datetime_str(moment()),
            "name": beautify_name(self.module.name),
            "data": self.info_email.__dict__,
            "filename": self.set_filepath()}
        self.html_content: SafeString = htmly.render(ctx)

    def set_copia_oculta(self):
        self.copia_oculta = config('EMAIL_BCC', cast=Csv())

    def set_destinatary(self):
        self.destinatary = [
            'desarrollador@logifarma.co',
            'logistica@logifarma.co'
        ]

    def set_filepath(self):
        """
        Si la ejecución es local self.module.filepath es:
          '/Users/jane/Downloads/myfile.csv'
        Si la ejecución es a través del drive self.module.filepath es:
          'myfile.csv'
        """
        fp: str = self.module.filepath.split('/')  # Cuando se lee un archivo local
        return fp[-1]

    def set_subject(self):
        self.subject = f"Migración de {beautify_name(self.module.name)} ejecutada {datetime_str(moment())}"

    def render_local_html(self, name: str = None) -> None:
        """Crea archivo html local que es el mismo
        enviado por correo. Ej.: 'dispensacion.html'
        Esta funcion podria ser usada por fines de tests.
        """
        if not self.html_content:
            self.make_html_content()
        if not name:
            name = self.module.name
        p = Path(f"{name}.html")
        p.write_text(self.html_content)
