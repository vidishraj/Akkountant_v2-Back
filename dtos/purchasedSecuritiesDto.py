from marshmallow import Schema, fields, validates, ValidationError
from enums.MsnEnum import MSNENUM


class PurchasedSecuritiesSchema(Schema):
    buyID = fields.Integer(dump_only=True)  # Exclude from input, include in output
    securityCode = fields.String(required=True, validate=lambda s: len(s) <= 250)
    buyQuant = fields.Integer(required=True)
    buyPrice = fields.Decimal(as_string=True, required=True, places=2)
    userID = fields.String(required=True, validate=lambda s: len(s) <= 100)
    securityType = fields.String(required=True)

    @validates("securityType")
    def validate_security_type(self, value):
        if value not in MSNENUM:
            raise ValidationError(f"{value} is not a valid security type. Must be one of {list(MSNENUM)}.")
