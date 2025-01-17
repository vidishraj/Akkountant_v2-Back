import os
import re
import hashlib
import shutil
import uuid
from decimal import Decimal, ROUND_DOWN

from marshmallow import ValidationError

from dtos.MSNListDto import MSNList
from enums.EmailRegexEnum import EmailRegexEnum
from utils.DateTimeUtil import DateTimeUtil
from utils.logger import Logger


class GenericUtil:
    _instance = None
    logger = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GenericUtil, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    @staticmethod
    def generate_reference_id(datetime_str, varchar_field, decimal_field):

        # Convert the decimal field to a string to include in the hash
        decimal_field = float(decimal_field)
        decimal_str = f"{decimal_field:.2f}"  # Keep 2 decimal places for consistency

        # Concatenate all fields into a single string
        combined_str = f"{datetime_str}|{varchar_field}|{decimal_str}"

        # Create an MD5 hash of the combined string
        reference_id = hashlib.md5(combined_str.encode()).hexdigest()

        return reference_id

    def extractDetailsFromEmail(self, emails, bankType):
        try:
            # Retrieve the regex pattern from the enum
            pattern = EmailRegexEnum[bankType].value
            cleanedMails = []
            conflicts = []
            for email in emails:
                matches = re.search(pattern, email)

                if matches:
                    # Extract matched details as a dictionary
                    details = matches.groupdict()
                    date = DateTimeUtil().convert_to_sql_datetime(details.get('transaction_date'), bankType)
                    description = details.get('merchant')
                    amount = details.get('amount_spent')
                    referenceID = GenericUtil().generate_reference_id(date, description, amount)
                    cleanedMails.append({
                        'reference': referenceID,
                        'date': date,
                        'description': description,
                        'amount': amount,
                    })
                else:
                    # Insert this into conflicts here
                    conflicts.append(email)
                    self.logger.error(f"No match found for: {email}")
            return cleanedMails, conflicts
        except KeyError:
            self.logger.error(f"Error: '{bankType}' is not a valid EmailRegexEnum member.")

    @staticmethod
    def emptyTemp():
        folderPath = os.getcwd() + "/tmp"
        # Delete the folder and all its contents
        shutil.rmtree(folderPath)
        # Recreate the empty folder
        os.makedirs(folderPath, exist_ok=True)

        # This seems to be a faster approach then deleting the files in the folder? Not sure

    @staticmethod
    def getFileSize(filePath):
        return os.path.getsize(os.getcwd() + '/tmp/' + filePath)

    @staticmethod
    def generate_custom_buyID():
        # Custom logic to generate unique IDs; adjust as needed
        return f"CUSTOM-{uuid.uuid4().hex[:8]}"  # Example: CUSTOM-ab12cd34

    @staticmethod
    def fetchStockRates(response):
        info = response.get('info', {})
        price_info = response.get('priceInfo', {})

        # Format data using Decimal quantization
        data = {
            "symbol": info.get('symbol', ''),
            "companyName": info.get('companyName', ''),
            "industry": info.get('industry', ''),
            "lastPrice": Decimal(price_info.get('lastPrice', 0)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            "change": Decimal(price_info.get('change', 0)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            "pChange": Decimal(price_info.get('pChange', 0)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            "previousClose": Decimal(price_info.get('previousClose', 0)).quantize(Decimal('0.01'),
                                                                                  rounding=ROUND_DOWN),
            "open": Decimal(price_info.get('open', 0)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            "close": Decimal(price_info.get('close', 0)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            "dayHigh": Decimal(price_info.get('intraDayHighLow', {}).get('max', 0)).quantize(Decimal('0.01'),
                                                                                             rounding=ROUND_DOWN),
            "dayLow": Decimal(price_info.get('intraDayHighLow', {}).get('min', 0)).quantize(Decimal('0.01'),
                                                                                            rounding=ROUND_DOWN),
        }
        msn_summary_schema = MSNList()
        try:
            result = msn_summary_schema.load(data)
            return result
        except ValidationError as err:
            return {'error': f'Validation Error {err}'}

    @staticmethod
    def convertToDecimal(num):
        return Decimal(num).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
