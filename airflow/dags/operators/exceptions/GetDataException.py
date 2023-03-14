class GetDataException(Exception):
    """
       Exception raised when importing data did not succeed.
       """

    def __init__(self, message=None):
        self.message = message or "Error Occurred"
