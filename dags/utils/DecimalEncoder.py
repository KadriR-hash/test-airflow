"""from decimal import Decimal
from json import JSONEncoder


class DecimalEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            obj = float(obj)
"""