from marshmallow import Schema, fields, validates, ValidationError
from datetime import datetime
from enums import ServiceTypeEnum


class UserTokenSchema(Schema):
    user_id = fields.String(required=True)
    access_token = fields.String(required=True)
    refresh_token = fields.String(required=True)
    client_id = fields.String(required=True)
    client_secret = fields.String(required=True)
    expiry = fields.Integer(required=True)
    service_type = fields.String(required=True)

    @validates("service_type")
    def validate_service_type(self, value):
        if value not in ServiceTypeEnum:
            raise ValidationError(f"{value} is not a valid service type. Must be one of {list(ServiceTypeEnum)}.")
