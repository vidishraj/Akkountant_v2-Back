from abc import ABC

import requests
from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN


class MfService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"

    def fetchAllSecurities(self):
        return self.check_and_update_file(self.baseDirectory + 'assets', 'MFList', self.baseAPIURL + 'mf')

    def findSecurity(self, securityCode):
        try:
            # Step 1: Make the API request
            response = requests.get(f"{self.baseAPIURL}/mf/{securityCode}")
            response.raise_for_status()  # Raise an error if the request failed

            # Step 2: Parse the JSON response
            data = response.json()

            # Step 3: Check if the status is SUCCESS
            if data.get("status") != "SUCCESS":
                raise ValidationError("Failed to fetch security data. Status not successful.")

            # Step 4: Extract the latest NAV
            nav_data = data.get("data", [])
            if not nav_data:
                raise ValidationError("No NAV data found for the specified security code.")

            # Assuming the latest NAV is the first element in the list
            latest_nav = nav_data[0].get("nav")

            return {"latest_nav": latest_nav, "security_code": securityCode}

        except requests.RequestException as e:
            raise ValidationError(f"API request failed: {str(e)}")
        except (KeyError, IndexError):
            raise ValidationError("Invalid data format received from the API.")

    def buySecurity(self, security_data, filePath, key, userId):
        try:
            # Validate the securityCode using the separate function
            self.validate_security_in_json(filePath, key, security_data['securityCode'])

            # Proceed with insertion if validation passes
            new_purchase = PurchasedSecurities(
                securityCode=security_data['securityCode'],
                buyQuant=security_data['buyQuant'],
                buyPrice=security_data['buyPrice'],
                userID=security_data['userID'],
                securityType=security_data['securityType']
            )

            self.db.session.add(new_purchase)
            self.db.session.commit()
            return {"message": "Security purchased successfully", "buyID": new_purchase.buyID}

        except ValidationError as e:
            return {"error": str(e)}

    def sellSecurity(self, sell_data, userId):
        try:
            # Fetch the corresponding purchase record
            purchase = self.db.session.query(PurchasedSecurities).filter_by(buyID=sell_data['buyID']).one()

            if sell_data['sellQuant'] > purchase.buyQuant:
                return {"error": "Sell quantity exceeds available quantity"}

            # Calculate profit
            profit = (sell_data['sellQuant'] * sell_data['sellPrice']) - (sell_data['sellQuant'] * purchase.buyPrice)

            # Insert into SoldSecurities
            new_sale = SoldSecurities(
                buyID=sell_data['buyID'],
                sellQuant=sell_data['sellQuant'],
                sellPrice=sell_data['sellPrice'],
                profit=profit
            )

            self.db.session.add(new_sale)
            self.db.session.commit()
            return {"message": "Security sold successfully", "sellID": new_sale.sellID, "profit": profit}

        except NoResultFound:
            return {"error": "Purchase record not found for the given buyID"}
