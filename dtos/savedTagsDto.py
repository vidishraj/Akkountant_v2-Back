from marshmallow import Schema, fields


class SavedTagsSchema(Schema):
    details = fields.String(required=True)
    tag = fields.Date(required=True)
    user = fields.String(required=True)
