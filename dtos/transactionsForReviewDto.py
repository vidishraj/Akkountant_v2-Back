from marshmallow import Schema, fields


class TransactionForReviewSchema(Schema):
    user = fields.String(required=True)
    conflictID = fields.Integer(required=True)
    conflict = fields.String(required=True, validate=lambda s: len(s) <= 700)
