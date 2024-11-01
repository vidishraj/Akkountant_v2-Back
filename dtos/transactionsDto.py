from marshmallow import Schema, fields, validates, ValidationError

from enums.BanksEnum import BankEnums


class TransactionsSchema(Schema):
    referenceID = fields.String(required=True, validate=lambda s: len(s) <= 64)
    date = fields.Date(required=True, format="%Y-%m-%d")
    details = fields.String(required=True, validate=lambda s: len(s) <= 500)
    amount = fields.Decimal(as_string=True, required=True, places=2)
    tag = fields.String(validate=lambda s: len(s) <= 100)
    fileID = fields.String(validate=lambda s: len(s) <= 100)
    source = fields.String(validate=lambda s: s == "EMAIL" or s == "STATEMENT")
    bank = fields.String(required=True)
    user = fields.String(required=True, validate=lambda s: len(s) <= 100)

    @validates("bank")
    def validate_service_type(self, value):
        if value not in BankEnums:
            raise ValidationError(f"{value} is not a valid service type. Must be one of {list(BankEnums)}.")
