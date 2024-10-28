from marshmallow import Schema, fields


class FileDetailsSchema(Schema):
    fileID = fields.String(required=True)
    uploadDate = fields.Date(required=True)
    fileName = fields.String(required=True)
    fileSize = fields.String(required=True, validate=lambda s: len(s) <= 64)
    statementCount = fields.Integer(required=True)
    bank = fields.String(required=True)
    user = fields.String(required=True)
