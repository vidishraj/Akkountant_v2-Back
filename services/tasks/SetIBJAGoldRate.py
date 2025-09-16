import os
import re
import requests
from datetime import datetime, timedelta
import PyPDF2
from io import BytesIO
from bs4 import BeautifulSoup

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
        Fetches and parses IBJA gold and silver rates from PDF by scraping the homepage
        """
        try:
            # First, scrape the IBJA homepage to find the PDF link
            pdf_url = self.find_pdf_from_homepage()
            
            if not pdf_url:
                raise Exception("Could not find PDF link on IBJA homepage")
            
            self.logger.info(f"Found PDF URL: {pdf_url}")
            
            # Fetch the PDF
            response = requests.get(pdf_url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
            }, timeout=30)
            
            if response.status_code == 200:
                # Parse the PDF content
                rates_data = self.parse_ibja_pdf(response.content)
                if rates_data:
                    self.logger.info("Successfully parsed IBJA rates from homepage PDF")
                    return rates_data
                else:
                    raise Exception("Failed to parse PDF content")
            else:
                raise Exception(f"Failed to fetch PDF, status: {response.status_code}")
            
        except Exception as e:
            self.logger.error(f"Error fetching IBJA data: {str(e)}")
            raise

    def find_pdf_from_homepage(self):
        """
        Scrape the IBJA homepage to find the "Previous 30 days" PDF link
        """
        try:
            homepage_url = "https://ibjarates.com/"
            self.logger.info(f"Scraping IBJA homepage: {homepage_url}")
            
            response = requests.get(homepage_url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
            }, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch homepage, status: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for "Previous 30 days" button or link
            # This could be in various forms: button, link, etc.
            pdf_link = None
            
            # Try different selectors to find the PDF link
            selectors = [
                'a[href*="30DaysPdf"]',  # Links containing 30DaysPdf
                'a[href*="pdf"]',  # Any PDF links
                'button[onclick*="30DaysPdf"]',  # Buttons with PDF onclick
                'a:contains("Previous 30 days")',  # Text-based search
                'a:contains("30 days")',  # Partial text search
                'a:contains("PDF")',  # PDF text search
            ]
            
            for selector in selectors:
                try:
                    if ':contains(' in selector:
                        # For text-based selectors, use find with string search
                        if 'Previous 30 days' in selector:
                            links = soup.find_all('a', string=re.compile(r'Previous.*30.*days', re.I))
                        elif '30 days' in selector:
                            links = soup.find_all('a', string=re.compile(r'30.*days', re.I))
                        elif 'PDF' in selector:
                            links = soup.find_all('a', string=re.compile(r'PDF', re.I))
                        else:
                            links = []
                    else:
                        # For CSS selectors
                        links = soup.select(selector)
                    
                    for link in links:
                        href = link.get('href') or link.get('onclick', '')
                        if href:
                            # Extract PDF URL from href or onclick
                            if href.startswith('http'):
                                pdf_link = href
                            elif href.startswith('/'):
                                pdf_link = f"https://ibjarates.com{href}"
                            elif 'UploadedFiles' in href:
                                # Extract from onclick or relative path
                                if not href.startswith('http'):
                                    pdf_link = f"https://ibjarates.com/{href}"
                                else:
                                    pdf_link = href
                            
                            if pdf_link and '30DaysPdf' in pdf_link:
                                self.logger.info(f"Found PDF link: {pdf_link}")
                                return pdf_link
                                
                except Exception as e:
                    self.logger.debug(f"Error with selector {selector}: {str(e)}")
                    continue
            
            # If no specific link found, try to find any recent PDF in the page source
            # Look for PDF URLs in the HTML content
            pdf_urls = re.findall(r'https?://[^"\s]+30DaysPdf[^"\s]+\.pdf', response.text)
            if pdf_urls:
                pdf_link = pdf_urls[0]  # Take the first one
                self.logger.info(f"Found PDF URL in page source: {pdf_link}")
                return pdf_link
            
            self.logger.warning("Could not find PDF link on homepage")
            return None
            
        except Exception as e:
            self.logger.error(f"Error scraping homepage: {str(e)}")
            return None

    def construct_ibja_url(self, date):
        """
        Construct IBJA PDF URL for a given date
        Format: https://ibjarates.com/UploadedFiles/30DaysPdf/Pdf_4038_YYYYMMDDHHMMSS_Daily%20Opening%20and%20Closing%20Market%20Rate.pdf
        """
        date_str = date.strftime('%Y%m%d')
        
        # Try both new and old PDF IDs
        pdf_ids = ["4038", "9447"]  # New format first, then fallback to old
        
        # Try multiple common time patterns for each PDF ID
        time_patterns = [
            "121048339",  # New observed pattern
            "170724682",  # Old pattern
            "170000000",  # 5 PM
            "120000000",  # 12 PM  
            "180000000",  # 6 PM
            "160000000",  # 4 PM
        ]
        
        # Try each PDF ID with each time pattern
        for pdf_id in pdf_ids:
            for time_str in time_patterns:
                url = f"https://ibjarates.com/UploadedFiles/30DaysPdf/Pdf_{pdf_id}_{date_str}{time_str}_Daily%20Opening%20and%20Closing%20Market%20Rate.pdf"
                try:
                    # Quick HEAD request to check if URL exists
                    response = requests.head(url, timeout=10)
                    if response.status_code == 200:
                        return url
                except:
                    continue
        
        # Fallback to the new pattern
        return f"https://ibjarates.com/UploadedFiles/30DaysPdf/Pdf_4038_{date_str}121048339_Daily%20Opening%20and%20Closing%20Market%20Rate.pdf"

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
                        gold_995_avg = (int(rates[2]) + int(rates[3])) // 2
                        gold_585_avg = (int(rates[8]) + int(rates[9])) // 2
                        silver_999_avg = (int(rates[10]) + int(rates[11])) // 2
                        
                        # Calculate GST inclusive rates (average + 3% GST)
                        gold_999_gst = int(gold_999_avg * 1.03)
                        gold_916_gst = int(gold_916_avg * 1.03)
                        gold_750_gst = int(gold_750_avg * 1.03)
                        gold_995_gst = int(gold_995_avg * 1.03)
                        gold_585_gst = int(gold_585_avg * 1.03)
                        silver_999_gst = int(silver_999_avg * 1.03)
                        
                        # Create backward compatible format with enhanced data
                        rate_data = {
                            # Backward compatibility - existing code expects these keys
                            "24 Carat": gold_999_gst,  # 999 purity = 24 carat (with GST)
                            "22 Carat": gold_916_gst,  # 916 purity = 22 carat (with GST)
                            "18 Carat": gold_750_gst,  # 750 purity = 18 carat (with GST)
                            
                            # Enhanced IBJA data structure
                            "ibja_data": {
                                "date": date_str,
                                "gold": {
                                    "999": {
                                        "am_price_10g": int(rates[0]),
                                        "pm_price_10g": int(rates[1]),
                                        "avg_price_10g": gold_999_avg,
                                        "avg_with_gst": gold_999_gst
                                    },
                                    "995": {
                                        "am_price_10g": int(rates[2]),
                                        "pm_price_10g": int(rates[3]),
                                        "avg_price_10g": gold_995_avg,
                                        "avg_with_gst": gold_995_gst
                                    },
                                    "916": {
                                        "am_price_10g": int(rates[4]),
                                        "pm_price_10g": int(rates[5]),
                                        "avg_price_10g": gold_916_avg,
                                        "avg_with_gst": gold_916_gst
                                    },
                                    "750": {
                                        "am_price_10g": int(rates[6]),
                                        "pm_price_10g": int(rates[7]),
                                        "avg_price_10g": gold_750_avg,
                                        "avg_with_gst": gold_750_gst
                                    },
                                    "585": {
                                        "am_price_10g": int(rates[8]),
                                        "pm_price_10g": int(rates[9]),
                                        "avg_price_10g": gold_585_avg,
                                        "avg_with_gst": gold_585_gst
                                    }
                                },
                                "silver": {
                                    "999": {
                                        "am_price_1kg": int(rates[10]),
                                        "pm_price_1kg": int(rates[11]),
                                        "avg_price_1kg": silver_999_avg,
                                        "avg_with_gst": silver_999_gst
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