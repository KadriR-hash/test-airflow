class CleanDataException(Exception):
    """
       Exception raised when the data cleansing did not succeed.
       """

    def __init__(self, message=None):
        self.message = message or "Error Occurred"


