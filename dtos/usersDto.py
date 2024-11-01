from marshmallow import Schema, fields, validates

from enums import BanksEnum
from utils.FirebaseAuthenticator import FirebaseAuthenticator


class UserSchema(Schema):
    userID = fields.String(required=True)
    email = fields.String(required=True)
    optedBanks = fields.String(required=True)

    # Config that the optedBanks are the allowed banks
    @validates("optedBanks")
    def checkBanks(self, value: str):
        value = value.split(',')
        for val in value:
            if val not in BanksEnum:
                return False
        return True

    # Confirm that the userID is a registered firebase user
    @validates("userID")
    def checkUserID(self, value):
        try:
            FirebaseAuthenticator().verify_token(value)
            return True
        except:
            return False
