class CustomValidationError(Exception):
    def __init__(self, error):
        super().__init__(error)
        self.error = error
