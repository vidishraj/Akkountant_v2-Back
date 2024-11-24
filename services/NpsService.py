from abc import ABC

import requests
from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN


class NPSService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://nps.purifiedbytes.com/api/"

    def fetchAllSecurities(self):
        return self.check_and_update_file(self.baseDirectory + 'assets', 'NPSList', self.baseAPIURL + 'schemes.json')

    def findSecurity(self, securityCode):
        try:
            # Step 1: Make the API request
            response = requests.get(f"{self.baseAPIURL}/api/schemes/{securityCode}/nav.json")
            response.raise_for_status()  # Raise an error if the request failed

            # Step 2: Parse the JSON response
            data = response.json()

            # Step 3: Extract the NAV data
            nav_data = data.get("data", [])
            if not nav_data:
                raise ValidationError("No NAV data found for the specified NPS security code.")

            # Step 4: Extract the latest NAV (first element in the list)
            latest_nav = nav_data[0].get("nav")
            if latest_nav is None:
                raise ValidationError("Invalid NAV data format in the response.")

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
