from marshmallow import Schema, fields


class TransactionsSchema(Schema):
    referenceID = fields.String(required=True, validate=lambda s: len(s) <= 64)
    date = fields.Date(required=True, format="%Y-%m-%d")
    details = fields.String(required=True, validate=lambda s: len(s) <= 500)
    amount = fields.Decimal(as_string=True, required=True, places=2)
    tag = fields.String(validate=lambda s: len(s) <= 100)
    fileID = fields.String(validate=lambda s: len(s) <= 100)
    source = fields.String(validate=lambda s: s == "EMAIL" or s == "STATEMENT")
    user = fields.String(required=True, validate=lambda s: len(s) <= 100)
