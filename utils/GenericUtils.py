import os
import re
import hashlib
import shutil
import uuid
import json
import subprocess
import tempfile
import fitz  # PyMuPDF for password-protected PDF handling
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
        """
        Primary email processing method - Claude Code first, regex fallback
        """
        try:
            cleanedMails = []
            conflicts = []
            
            for email in emails:
                # PRIMARY: Try Claude Code processing first
                claude_result = self._try_claude_extraction(email, bankType)
                if claude_result:
                    cleanedMails.append(claude_result)
                    self.logger.info(f"Claude Code successfully processed email from {bankType}")
                    continue
                
                # FALLBACK: Try legacy regex pattern matching
                try:
                    pattern = EmailRegexEnum[bankType].value
                    matches = re.search(pattern, email['message'])
                    
                    if bankType == EmailRegexEnum.Millenia_Credit.name and matches is None:
                        matches = re.search(
                            r"Dear Customer, Thank you for using HDFC Bank Card (?P<card_number>XX\d{4}) for Rs\. (?P<amount_spent>[\d,]+(?:\.\d+)?) at (?P<merchant>.+?) on (?P<transaction_date>\d{2}-\d{2}-\d{4}) (?P<transaction_time>\d{2}:\d{2}:\d{2}) Authorization code:- (?P<authorization_code>\d+)",
                            email['message'])

                    if matches:
                        # Extract matched details as a dictionary
                        details = matches.groupdict()
                        date = DateTimeUtil().convert_to_sql_datetime(details.get('transaction_date'), bankType)
                        description = details.get('merchant')
                        amount = details.get('amount_spent')
                        referenceID = GenericUtil().generate_reference_id(email['time'], description, amount)
                        cleanedMails.append({
                            'reference': referenceID,
                            'date': date,
                            'description': description,
                            'amount': amount,
                            'processed_via': 'pattern_match'
                        })
                        self.logger.info(f"Regex fallback successfully processed email from {bankType}")
                    else:
                        # Both methods failed
                        conflicts.append(email['message'])
                        self.logger.error(f"Both Claude and regex failed for: {email['message'][:100]}...")
                        
                except KeyError:
                    # No regex pattern exists for this bank type - rely on Claude only
                    conflicts.append(email['message'])
                    self.logger.error(f"No regex pattern for {bankType} and Claude failed: {email['message'][:100]}...")
                    
            return cleanedMails, conflicts
            
        except Exception as e:
            self.logger.error(f"Error in extractDetailsFromEmail: {str(e)}")
            return [], [email.get('message', '') for email in emails]

    def _try_claude_pdf_extraction(self, pdf_path, bank_type, password=None):
        """Try to extract transaction data from password-protected PDF using Claude Code"""
        try:
            # Step 1: Extract text from password-protected PDF
            pdf_text = self._extract_pdf_text_with_password(pdf_path, password)
            if not pdf_text:
                self.logger.error("Failed to extract text from password-protected PDF")
                return None
            
            # Step 2: Create temporary text file for Claude
            temp_dir = "/tmp/akkountant_pdfs"
            os.makedirs(temp_dir, exist_ok=True)
            
            text_file = f"{temp_dir}/statement_{hash(pdf_path)}.txt"
            with open(text_file, 'w', encoding='utf-8') as f:
                f.write(f"Bank Statement from {bank_type}\n")
                f.write("="*50 + "\n")
                f.write(pdf_text)
            
            # Step 3: Send extracted text to Claude
            prompt = f'''Analyze this bank statement text and extract ALL transactions. Return ONLY valid JSON:
{{
    "transactions_found": true/false,
    "bank_name": "{bank_type}",
    "transactions": [
        {{
            "date": "YYYY-MM-DD",
            "description": "transaction description",
            "amount": "0.00",
            "type": "debit/credit",
            "balance": "0.00"
        }}
    ]
}}'''

            cmd = ['claude', 'code', prompt]
            with open(text_file, 'r') as f:
                result = subprocess.run(cmd, stdin=f, capture_output=True, text=True, timeout=120)
            
            # Cleanup
            os.remove(text_file)
            
            if result.returncode == 0:
                data = json.loads(result.stdout.strip())
                if data.get('transactions_found') and data.get('transactions'):
                    transactions = []
                    for txn in data['transactions']:
                        # Convert Claude response to expected format
                        amount = str(txn['amount']).replace(',', '')
                        # Convert to negative for debits (following existing convention)
                        if txn.get('type', '').lower() == 'debit':
                            amount = f"-{amount}" if not amount.startswith('-') else amount
                        
                        referenceID = GenericUtil().generate_reference_id(
                            txn['date'], 
                            txn['description'], 
                            float(amount.replace('-', ''))
                        )
                        
                        transactions.append({
                            'reference': referenceID,
                            'date': txn['date'],
                            'description': txn['description'],
                            'amount': amount,
                            'processed_via': 'claude_code'
                        })
                    
                    return transactions
            return None
        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            self.logger.error(f"Claude Code PDF extraction failed: {str(e)}")
            return None
    
    def _extract_pdf_text_with_password(self, pdf_path, password):
        """Extract text from password-protected PDF using PyMuPDF"""
        try:
            # Open the PDF with password
            doc = fitz.open(pdf_path)
            
            # Check if PDF is encrypted and needs password
            if doc.needs_pass:
                if not password:
                    self.logger.error("PDF is password-protected but no password provided")
                    return None
                    
                # Authenticate with password
                auth_result = doc.authenticate(password)
                if not auth_result:
                    self.logger.error("PDF password authentication failed")
                    return None
            
            # Extract text from all pages
            full_text = ""
            for page_num in range(doc.page_count):
                page = doc[page_num]
                page_text = page.get_text()
                full_text += f"\n--- Page {page_num + 1} ---\n"
                full_text += page_text
            
            doc.close()
            
            if len(full_text.strip()) < 100:  # Sanity check
                self.logger.warning("Extracted text is too short, might be corrupted")
                return None
                
            self.logger.info(f"Successfully extracted {len(full_text)} characters from PDF")
            return full_text
            
        except Exception as e:
            self.logger.error(f"Failed to extract text from PDF: {str(e)}")
            return None

    def _try_claude_extraction(self, email, bankType):
        """Try to extract transaction data using Claude Code as fallback"""
        try:
            temp_dir = "/tmp/akkountant_emails"
            os.makedirs(temp_dir, exist_ok=True)
            
            email_file = f"{temp_dir}/email_{hash(email['message'])}.txt"
            with open(email_file, 'w', encoding='utf-8') as f:
                f.write(f"Subject: {email.get('subject', '')}\n")
                f.write(f"Time: {email.get('time', '')}\n")
                f.write(f"Body: {email.get('message', '')}\n")

            prompt = '''Extract transaction data from this banking email. Return ONLY valid JSON:
{
    "transaction_found": true/false,
    "transaction_date": "YYYY-MM-DD",
    "amount": "0.00",
    "merchant": "merchant name",
    "description": "transaction description"
}'''

            cmd = ['claude', 'code', prompt]
            with open(email_file, 'r') as f:
                result = subprocess.run(cmd, stdin=f, capture_output=True, text=True, timeout=60)
            
            os.remove(email_file)
            
            if result.returncode == 0:
                data = json.loads(result.stdout.strip())
                if data.get('transaction_found'):
                    # Convert Claude response to expected format
                    amount = str(data['amount']).replace(',', '')
                    referenceID = GenericUtil().generate_reference_id(
                        data['transaction_date'], 
                        data['description'], 
                        float(amount)
                    )
                    return {
                        'reference': referenceID,
                        'date': data['transaction_date'],
                        'description': data['description'],
                        'amount': amount,
                        'processed_via': 'claude_code'
                    }
            return None
        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            self.logger.error(f"Claude Code extraction failed: {str(e)}")
            return None

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
