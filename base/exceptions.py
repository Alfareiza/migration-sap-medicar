
class RetryMaxException(Exception):
    def __init__(self, message="Se han realizado varios intentos pero no hubo respuesta exitosa"):
        self.message = message
        super().__init__(self.message)
