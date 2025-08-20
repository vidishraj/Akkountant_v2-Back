import os
import re
import requests
from datetime import datetime, timedelta
import PyPDF2
from io import BytesIO

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetIBJAGoldRate(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetIBJAGoldRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 300

    def run(self):
        try:
            # Get IBJA gold and silver rates from PDF
            jsonData = self.getIBJAData()
            try:
                filePath = os.path.join(self.tmp_dir, 'GOLDRATE.json')
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.GoldRatePrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.GoldRatePrefix,
                                                              self.jsonService.ratesType)

                fileMoved = self.move_file(filePath, latestFilePath)

                if fileMoved:
                    # delete old file
                    self.jsonService.deleteFile(latestFile)
                else:
                    return 'Failed to move file', "Failed", self.interval
                return 'Completed successfully', "Completed", self.interval
            except Exception as ex:
                return ex.__str__(), "Failed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def getIBJAData(self):
        """
        Fetches and parses IBJA gold and silver rates from PDF
        """
        try:
            # Try current date first, then previous days if not available
            for days_back in range(7):  # Try up to 7 days back
                target_date = datetime.now() - timedelta(days=days_back)
                pdf_url = self.construct_ibja_url(target_date)
                
                self.logger.info(f"Attempting to fetch IBJA rates from: {pdf_url}")
                
                response = requests.get(pdf_url, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
                }, timeout=30)
                
                if response.status_code == 200:
                    # Parse the PDF content
                    rates_data = self.parse_ibja_pdf(response.content)
                    if rates_data:
                        self.logger.info(f"Successfully parsed IBJA rates from {target_date.strftime('%Y-%m-%d')}")
                        return rates_data
                else:
                    self.logger.warning(f"Failed to fetch PDF for {target_date.strftime('%Y-%m-%d')}, status: {response.status_code}")
            
            raise Exception("Could not fetch IBJA rates for any recent date")
            
        except Exception as e:
            self.logger.error(f"Error fetching IBJA data: {str(e)}")
            raise

    def construct_ibja_url(self, date):
        """
        Construct IBJA PDF URL for a given date
        Format: https://ibjarates.com/UploadedFiles/30DaysPdf/Pdf_9447_YYYYMMDDHHMMSS_Daily%20Opening%20and%20Closing%20Market%20Rate.pdf
        """
        date_str = date.strftime('%Y%m%d')
        # Try multiple common time patterns
        time_patterns = [
            "170724682",  # From the example
            "170000000",  # 5 PM
            "120000000",  # 12 PM  
            "180000000",  # 6 PM
            "160000000",  # 4 PM
        ]
        
        # Try each time pattern
        for time_str in time_patterns:
            url = f"https://ibjarates.com/UploadedFiles/30DaysPdf/Pdf_9447_{date_str}{time_str}_Daily%20Opening%20and%20Closing%20Market%20Rate.pdf"
            try:
                # Quick HEAD request to check if URL exists
                response = requests.head(url, timeout=10)
                if response.status_code == 200:
                    return url
            except:
                continue
        
        # Fallback to the example pattern
        return f"https://ibjarates.com/UploadedFiles/30DaysPdf/Pdf_9447_{date_str}170724682_Daily%20Opening%20and%20Closing%20Market%20Rate.pdf"

    def parse_ibja_pdf(self, pdf_content):
        """
        Parse the IBJA PDF content and extract gold/silver rates
        """
        try:
            pdf_file = BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text_content = ""
            for page in pdf_reader.pages:
                text_content += page.extract_text()
            
            # Parse the text content to extract rates
            rates_data = self.extract_rates_from_text(text_content)
            return rates_data
            
        except Exception as e:
            self.logger.error(f"Error parsing IBJA PDF: {str(e)}")
            return None

    def extract_rates_from_text(self, text):
        """
        Extract gold and silver rates from PDF text content
        """
        try:
            lines = text.split('\n')
            
            # Find the most recent date entry (first non-weekend/holiday entry)
            for line in lines:
                # Look for date pattern like "19-Aug-25" followed by rate data
                date_match = re.search(r'(\d{1,2}-[A-Za-z]{3}-\d{2})\s+(.+)', line)
                if date_match:
                    date_str = date_match.group(1)
                    rates_str = date_match.group(2)
                    
                    # Skip weekend/holiday entries
                    if any(skip_word in rates_str.upper() for skip_word in ['SUN', 'SAT', 'HOLIDAY']):
                        continue
                    
                    # Extract rates (expecting 12 values: 10 gold + 2 silver)
                    rates = re.findall(r'\d+', rates_str)
                    
                    if len(rates) >= 12:
                        # Calculate average prices for backward compatibility
                        gold_999_avg = (int(rates[0]) + int(rates[1])) // 2
                        gold_916_avg = (int(rates[4]) + int(rates[5])) // 2
                        gold_750_avg = (int(rates[6]) + int(rates[7])) // 2
                        
                        # Create backward compatible format with enhanced data
                        rate_data = {
                            # Backward compatibility - existing code expects these keys
                            "24 Carat": gold_999_avg,  # 999 purity = 24 carat
                            "22 Carat": gold_916_avg,  # 916 purity = 22 carat  
                            "18 Carat": gold_750_avg,  # 750 purity = 18 carat
                            
                            # Enhanced IBJA data structure
                            "ibja_data": {
                                "date": date_str,
                                "gold": {
                                    "999": {
                                        "am_price_10g": int(rates[0]),
                                        "pm_price_10g": int(rates[1]),
                                        "avg_price_10g": gold_999_avg
                                    },
                                    "995": {
                                        "am_price_10g": int(rates[2]),
                                        "pm_price_10g": int(rates[3]),
                                        "avg_price_10g": (int(rates[2]) + int(rates[3])) // 2
                                    },
                                    "916": {
                                        "am_price_10g": int(rates[4]),
                                        "pm_price_10g": int(rates[5]),
                                        "avg_price_10g": gold_916_avg
                                    },
                                    "750": {
                                        "am_price_10g": int(rates[6]),
                                        "pm_price_10g": int(rates[7]),
                                        "avg_price_10g": gold_750_avg
                                    },
                                    "585": {
                                        "am_price_10g": int(rates[8]),
                                        "pm_price_10g": int(rates[9]),
                                        "avg_price_10g": (int(rates[8]) + int(rates[9])) // 2
                                    }
                                },
                                "silver": {
                                    "999": {
                                        "am_price_1kg": int(rates[10]),
                                        "pm_price_1kg": int(rates[11]),
                                        "avg_price_1kg": (int(rates[10]) + int(rates[11])) // 2
                                    }
                                },
                                "currency": "INR",
                                "source": "IBJA"
                            }
                        }
                        
                        self.logger.info(f"Extracted rates for {date_str}")
                        return rate_data
            
            self.logger.warning("No valid rate data found in PDF text")
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting rates from text: {str(e)}")
            return None