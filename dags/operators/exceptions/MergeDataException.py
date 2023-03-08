class MergeDataException(Exception):
    """
       Exception raised when the merge did not succeed.
       """

    def __init__(self, message=None):
        self.message = message or "Error Occurred"
