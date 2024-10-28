from marshmallow import Schema, fields


class StatementPasswordsSchema(Schema):
    bank = fields.String(required=True, validate=lambda s: len(s) <= 100)
    password_hash = fields.String(required=True)
    user = fields.String(required=True, validate=lambda s: len(s) <= 100)
