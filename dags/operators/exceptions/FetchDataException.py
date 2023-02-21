class FetchDataException(Exception):
    """
       Exception raised when fetching data did not succeed.
       """

    def __init__(self, message=None):
        self.message = message or "Error Occurred"

