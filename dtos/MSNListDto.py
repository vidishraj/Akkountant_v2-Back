from marshmallow import Schema, fields


class MSNList(Schema):
    symbol = fields.String(required=True)
    companyName = fields.String(required=True)
    industry = fields.String(required=True)
    lastPrice = fields.Decimal(required=True, validate=lambda s: s >= 0)
    change = fields.Decimal(required=True)
    pChange = fields.Decimal(required=True)
    previousClose = fields.Decimal(required=True, validate=lambda s: s >= 0)
    open = fields.Decimal(required=True, validate=lambda s: s >= 0)
    close = fields.Decimal(required=True, validate=lambda s: s >= 0)
    dayHigh = fields.Decimal(required=True, validate=lambda s: s >= 0)
    dayLow = fields.Decimal(required=True, validate=lambda s: s >= 0)
