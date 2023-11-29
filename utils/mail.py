from pathlib import Path
from smtplib import SMTPSenderRefused
from typing import List

from decouple import config, Csv
from django.core.mail import EmailMessage
from django.template.loader import get_template
from django.utils.safestring import SafeString

from core import settings
from core.settings import BASE_DIR, logger
from utils.resources import datetime_str, moment, beautify_name


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
            logger.info(f"Adjuntando {attachment}")
            size += Path(attachment).stat().st_size
            if size <= 25_000_000:
                self.attach_file(attachment)
            else:
                logger.warning(f"No fue posible adjuntar {attachment} por su peso")

    def send(self):
        """
        from django.core.mail import EmailMessage

        email = EmailMessage( "Hello", "Body goes here", "alfareiza@gmail.com", ["alfareiza@gmail.com"])
        """
        self.prepare_email()
        try:
            if r := self.email.send(fail_silently=False):
                logger.info(f'Reporte de e-mail enviado referente al archivo {self.module.filepath}')
            else:
                logger.warning(f'E-mail no enviado referente al archivo {self.module.filepath}')
        except SMTPSenderRefused as e:
            logger.warning(f'E-mail no enviado porque {e}')
        except Exception as e:
            logger.warning(f'E-mail no enviado. Error={e}')
        finally:
            print(20 * ' ')

    def attach_file(self, filepath):
        self.email.attach_file(str(filepath))

    def make_html_content(self):
        self.info_email.group_by_type_of_errors()
        htmly = get_template(self.template)  # Create a <django.template.backends.django.Template> Object
        ctx = {
            "fecha": datetime_str(moment()),
            "name": beautify_name(self.module.name),
            "data": self.info_email.__dict__,
            "filename": self.module.filepath
        }
        self.html_content: SafeString = htmly.render(ctx)

    def set_copia_oculta(self):
        self.copia_oculta = config('EMAIL_BCC', cast=Csv())

    def set_destinatary(self):
        self.destinatary = [
            'desarrollador@logifarma.co',
            'logistica@logifarma.co'
        ]

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
