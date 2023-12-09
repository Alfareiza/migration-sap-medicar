
class ArchivoExcedeCantidadDocumentos(Exception):
    def __init__(self, message="El archivo a ser procesado excede la cantidad de lineas permitidas"):
        self.message = message
        super().__init__(self.message)
