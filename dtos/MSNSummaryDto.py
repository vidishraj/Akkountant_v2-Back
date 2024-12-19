from marshmallow import Schema, fields


class MSNSummary(Schema):
    totalValue = fields.Decimal(required=True)
    currentValue = fields.Decimal(required=True)
    changePercent = fields.Decimal(required=True)
    changeAmount = fields.Decimal(required=True)
    count = fields.Integer(required=True, validate=lambda s: s >= 0)
    marketStatus = fields.Boolean(required=True)
