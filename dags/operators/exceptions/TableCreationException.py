class TableCreationException(Exception):
    """
       Exception raised when the table creation did not succeed.
       """

    def __init__(self, message=None):
        self.message = message or "Error Occurred"
