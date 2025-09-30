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
                            'processed_via': 'PATTERN_MATCH'
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
            prompt = f'''You are a data extraction tool. Analyze this bank statement text and extract ALL transactions. Respond with ONLY a JSON object. Do not include any explanatory text, markdown, or conversation.

Required JSON format:
{{
    "transactions_found": true,
    "bank_name": "{bank_type}",
    "transactions": [
        {{
            "date": "2025-09-25",
            "description": "AMAZON PURCHASE",
            "amount": "2500.00",
            "type": "debit",
            "balance": "15000.00"
        }}
    ]
}}

If no transactions found, return: {{"transactions_found": false}}

Bank statement text:'''

            # Combine prompt and PDF text
            combined_content = prompt + "\n\n" + pdf_text
            
            # For --print mode, pass content via stdin
            cmd = ['claude', 'code', '--print', '--output-format', 'json']
            result = subprocess.run(cmd, input=combined_content, capture_output=True, text=True, timeout=120)
            
            # Cleanup
            os.remove(text_file)
            
            if result.returncode == 0:
                # Try to extract JSON from Claude's response
                data = self._extract_json_from_response(result.stdout)
                if data and data.get('transactions_found') and data.get('transactions'):
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
                            'processed_via': 'CLAUDE_CODE'
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
            prompt = '''You are a data extraction tool. Extract transaction information from this banking email and respond with ONLY a JSON object. Do not include any explanatory text, markdown, or conversation.

Required JSON format:
{
    "transaction_found": true,
    "transaction_date": "2025-09-25",
    "amount": "2500.00",
    "merchant": "AMAZON",
    "description": "AMAZON transaction"
}

If no transaction is found, return: {"transaction_found": false}

Email content:'''

            # Combine prompt and email content
            combined_content = prompt + "\n\n" + f"Subject: {email.get('subject', '')}\nTime: {email.get('time', '')}\nBody: {email.get('message', '')}"
            
            # For --print mode, pass content via stdin
            cmd = ['claude', 'code', '--print', '--output-format', 'json']
            result = subprocess.run(cmd, input=combined_content, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                # Try to extract JSON from Claude's response
                json_data = self._extract_json_from_response(result.stdout)
                if json_data and json_data.get('transaction_found'):
                    # Convert Claude response to expected format
                    amount = str(json_data['amount']).replace(',', '')
                    referenceID = GenericUtil().generate_reference_id(
                        json_data['transaction_date'], 
                        json_data['description'], 
                        float(amount)
                    )
                    return {
                        'reference': referenceID,
                        'date': json_data['transaction_date'],
                        'description': json_data['description'],
                        'amount': amount,
                        'processed_via': 'CLAUDE_CODE'
                    }
            return None
        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            self.logger.error(f"Claude Code extraction failed: {str(e)}")
            return None

    def _extract_json_from_response(self, response_text):
        """Extract JSON from Claude's response, handling structured CLI output"""
        try:
            # First, try to parse as Claude CLI JSON response format
            cli_response = json.loads(response_text.strip())
            if isinstance(cli_response, dict) and 'result' in cli_response:
                # Extract the result field which contains the actual content
                result_content = cli_response['result']
                
                # Look for JSON within markdown code blocks
                import re
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', result_content, re.DOTALL)
                if json_match:
                    try:
                        return json.loads(json_match.group(1))
                    except json.JSONDecodeError:
                        pass
                
                # Try to parse the result content directly
                try:
                    return json.loads(result_content)
                except json.JSONDecodeError:
                    pass
                    
            # If it's already the transaction JSON
            elif isinstance(cli_response, dict) and 'transaction_found' in cli_response:
                return cli_response
                
        except json.JSONDecodeError:
            pass
        
        # Fallback: look for JSON within the text
        import re
        
        # Look for JSON within markdown code blocks first
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                if 'transaction_found' in data:
                    return data
            except json.JSONDecodeError:
                pass
        
        # Look for JSON object patterns
        json_patterns = [
            r'\{[^{}]*"transaction_found"[^{}]*\}',  # Simple JSON object
            r'\{(?:[^{}]|{[^{}]*})*\}',  # More complex nested JSON
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, response_text, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads(match)
                    if 'transaction_found' in data or 'transactions_found' in data:
                        return data
                except json.JSONDecodeError:
                    continue
        
        # If no JSON found, try to extract data manually from conversational response
        if 'Amount:' in response_text and 'Merchant:' in response_text:
            try:
                amount_match = re.search(r'Amount:\s*Rs\.?\s*([\d,]+(?:\.\d+)?)', response_text)
                merchant_match = re.search(r'Merchant:\s*([^\n]+)', response_text)
                date_match = re.search(r'Date:\s*(\d{2}-\d{2}-\d{4})', response_text)
                
                if amount_match and merchant_match and date_match:
                    return {
                        "transaction_found": True,
                        "transaction_date": date_match.group(1),
                        "amount": amount_match.group(1).replace(',', ''),
                        "merchant": merchant_match.group(1).strip(),
                        "description": f"{merchant_match.group(1).strip()} transaction"
                    }
            except Exception:
                pass
        
        self.logger.warning(f"Could not extract JSON from Claude response: {response_text[:200]}...")
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
