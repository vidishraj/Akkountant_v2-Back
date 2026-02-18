import os
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetNPSRate(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetNPSRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 60

    def run(self):
        try:
            # Generate NPS rate data from Excel downloads
            jsonData = self.fetchNPSRatesFromExcel()
            
            try:
                filePath = os.path.join(self.tmp_dir, 'NPSRATE.json')
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.NpsRatePrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.NpsRatePrefix, self.jsonService.ratesType)

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

    def fetchNPSRatesFromExcel(self):
        """
        Fetch NPS rates by downloading Excel files from npstrust.org.in
        Returns data in the same format as the old API with historical data
        """
        all_rates = []
        successful_downloads = 0
        
        # Iterate through all PFM IDs (PFM001 to PFM014)
        for pfm_num in range(1, 15):
            pfm_id = f"PFM0{pfm_num:02d}"
            
            # Try different schemes for this PFM (001 to 016)
            for scheme_num in range(1, 17):
                scheme_id = f"SM0{pfm_num:02d}{scheme_num:03d}"
                
                try:
                    excel_data = self.downloadNPSExcel(pfm_id, scheme_id)
                    if excel_data is not None and not excel_data.empty:
                        # Extract rates from this scheme
                        scheme_rates = self.extractRatesFromExcel(excel_data, scheme_id)
                        if scheme_rates:
                            all_rates.append(scheme_rates)
                            self.logger.info(f"Successfully processed rates for {scheme_id}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing rates for {scheme_id}: {str(e)}")
                    continue
        
        successful_downloads = len(all_rates)
        self.logger.info(f"Total scheme rates collected: {successful_downloads}")
        
        return {
            "data": all_rates,
            "total_schemes": successful_downloads
        }
    
    def downloadNPSExcel(self, pfm_id, scheme_id):
        """
        Download Excel file for a specific PFM and scheme
        """
        url = f"https://npstrust.org.in/scheme-wise-nav-report-excel?navcatdataxls={pfm_id}&navyearselxls=12&navsubdataxls={scheme_id}"
        
        try:
            response = requests.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            # Check if response contains Excel data
            if 'application/vnd' in response.headers.get('content-type', '') or len(response.content) > 1000:
                # The API returns TSV data despite the Excel content-type header
                tsv_data = pd.read_csv(BytesIO(response.content), sep='\t')
                return tsv_data
            else:
                # Likely an error page or no data
                return None
                
        except Exception as e:
            self.logger.debug(f"No data available for {pfm_id}/{scheme_id}: {str(e)}")
            return None
    
    def extractRatesFromExcel(self, excel_data, scheme_id):
        """
        Extract NAV rates with historical data from Excel
        Returns scheme rate data in the format expected by the system
        """
        try:
            # Ensure we have the required columns
            if not all(col in excel_data.columns for col in ['DATE OF NAV', 'NAV VALUE', 'SCHEME ID', 'SCHEME NAME']):
                self.logger.error(f"Missing required columns in TSV data for {scheme_id}")
                return None
            
            # Sort by date to get chronological order (newest first)
            excel_data['DATE OF NAV'] = pd.to_datetime(excel_data['DATE OF NAV'])
            excel_data = excel_data.sort_values('DATE OF NAV', ascending=False)
            
            if excel_data.empty:
                return None
                
            # Get the latest row
            latest_row = excel_data.iloc[0]
            
            # Build the rate object similar to old API format
            rate_data = {
                "scheme_id": scheme_id,
                "scheme_name": latest_row['SCHEME NAME'],
                "nav": float(latest_row['NAV VALUE']),
                "date": latest_row['DATE OF NAV'].strftime('%Y-%m-%d'),
                "pfm_id": latest_row.get('PFM ID', ''),
                "pfm_name": latest_row.get('PFM NAME', '')
            }
            
            # Add historical data if available
            try:
                # Yesterday's NAV (previous business day)
                if len(excel_data) > 1:
                    rate_data['yesterday'] = float(excel_data.iloc[1]['NAV VALUE'])
                
                # Last week (7 days ago, or closest available)
                week_ago_data = excel_data[excel_data['DATE OF NAV'] <= (latest_row['DATE OF NAV'] - timedelta(days=7))]
                if not week_ago_data.empty:
                    rate_data['lastWeek'] = float(week_ago_data.iloc[0]['NAV VALUE'])
                
                # Six months ago (180 days ago, or closest available)
                six_months_ago_data = excel_data[excel_data['DATE OF NAV'] <= (latest_row['DATE OF NAV'] - timedelta(days=180))]
                if not six_months_ago_data.empty:
                    rate_data['sixMonthsAgo'] = float(six_months_ago_data.iloc[0]['NAV VALUE'])
                    
            except Exception as e:
                self.logger.warning(f"Error adding historical data for {scheme_id}: {str(e)}")
            
            return rate_data
                
        except Exception as e:
            self.logger.error(f"Error extracting rates from Excel data for {scheme_id}: {str(e)}")
            return None
