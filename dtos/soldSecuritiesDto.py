from marshmallow import Schema, fields


class SoldSecuritiesSchema(Schema):
    sellID = fields.Integer(dump_only=True)  # Auto-incremented field
    buyID = fields.Integer(required=True)
    date = fields.Date(required=True, format="%Y-%m-%d")
    sellQuant = fields.Integer(required=True)
    sellPrice = fields.Decimal(as_string=True, required=True, places=2)
    profit = fields.Decimal(as_string=True, dump_only=True, places=2)  # Calculated field, not input
