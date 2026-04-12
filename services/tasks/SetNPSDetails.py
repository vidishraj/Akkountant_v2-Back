import os
import pandas as pd
import requests
from io import BytesIO

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetNPSDetails(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetNPSDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 90

    def run(self):
        try:
            # Generate NPS scheme details from Excel downloads
            jsonData = self.fetchNPSDetailsFromExcel()
            
            filePath = os.path.join(self.tmp_dir, 'NPSDetails.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.NpsListPrefix, self.jsonService.listType)
            if not ok:
                return err, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def fetchNPSDetailsFromExcel(self):
        """
        Fetch NPS scheme details by downloading Excel files from npstrust.org.in
        Returns data in the same format as the old API
        """
        all_schemes = []
        successful_downloads = 0
        
        # Iterate through all PFM IDs (PFM001 to PFM014)
        for pfm_num in range(1, 15):
            pfm_id = f"PFM0{pfm_num:02d}"
            pfm_schemes_found = []
            
            # Try all possible schemes for this PFM (001 to 016)
            for scheme_num in range(1, 17):
                scheme_id = f"SM0{pfm_num:02d}{scheme_num:03d}"
                
                try:
                    excel_data = self.downloadNPSExcel(pfm_id, scheme_id)
                    if excel_data is not None and not excel_data.empty:
                        # Extract schemes from this file
                        schemes = self.extractSchemesFromExcel(excel_data, pfm_id)
                        
                        # Add any new schemes we haven't seen before
                        for scheme in schemes:
                            if not any(existing['id'] == scheme['id'] for existing in pfm_schemes_found):
                                pfm_schemes_found.append(scheme)
                        
                        self.logger.debug(f"Found data for {scheme_id}")
                    else:
                        self.logger.debug(f"No data for {scheme_id}")
                        
                except Exception as e:
                    self.logger.debug(f"Error downloading {scheme_id}: {str(e)}")
                    continue
            
            if pfm_schemes_found:
                all_schemes.extend(pfm_schemes_found)
                successful_downloads += 1
                self.logger.info(f"Successfully processed {pfm_id} with {len(pfm_schemes_found)} schemes")
            else:
                self.logger.warning(f"No schemes found for {pfm_id}")
        
        # Remove any duplicate schemes across PFMs (based on scheme ID)
        unique_schemes = []
        seen_ids = set()
        for scheme in all_schemes:
            if scheme['id'] not in seen_ids:
                unique_schemes.append(scheme)
                seen_ids.add(scheme['id'])
            else:
                self.logger.debug(f"Removing duplicate scheme: {scheme['id']}")
        
        self.logger.info(f"Total unique schemes collected: {len(unique_schemes)} from {successful_downloads} PFMs")
        
        return {
            "data": unique_schemes,
            "total_schemes": len(unique_schemes),
            "successful_pfms": successful_downloads
        }
    
    def downloadNPSExcel(self, pfm_id, scheme_id):
        """
        Download Excel file for a specific PFM and scheme
        """
        url = f"https://npstrust.org.in/scheme-wise-nav-report-excel?navcatdataxls={pfm_id}&navyearselxls=12&navsubdataxls={scheme_id}"
        
        try:
            response = requests.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            # The API returns TSV data despite the Excel content-type header
            tsv_data = pd.read_csv(BytesIO(response.content), sep='\t')
            return tsv_data
            
        except Exception as e:
            self.logger.error(f"Error downloading Excel for {pfm_id}/{scheme_id}: {str(e)}")
            return None
    
    def extractSchemesFromExcel(self, excel_data, pfm_id):
        """
        Extract unique scheme details from Excel data
        Returns list of schemes in the format expected by the system
        """
        schemes = []
        
        try:
            # Get unique schemes from the TSV data
            unique_schemes = excel_data[['SCHEME ID', 'SCHEME NAME', 'PFM ID', 'PFM NAME']].drop_duplicates()
            
            for _, row in unique_schemes.iterrows():
                scheme = {
                    "id": row['SCHEME ID'],
                    "name": row['SCHEME NAME'],
                    "pfm_id": row['PFM ID'], 
                    "pfm_name": row['PFM NAME']
                }
                schemes.append(scheme)
                
        except Exception as e:
            self.logger.error(f"Error extracting schemes from Excel data: {str(e)}")
            
        return schemes
