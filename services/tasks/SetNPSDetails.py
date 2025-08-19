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
            
            try:
                filePath = self.tmp_dir + 'NPSDetails.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.listType, self.jsonService.NpsListPrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.NpsListPrefix, self.jsonService.listType)

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
            
            # Try to get schemes for this PFM by testing scheme 001
            # This will give us the PFM details
            test_scheme_id = f"SM0{pfm_num:02d}001"
            
            try:
                excel_data = self.downloadNPSExcel(pfm_id, test_scheme_id)
                if excel_data is not None and not excel_data.empty:
                    # Extract unique schemes from this PFM
                    pfm_schemes = self.extractSchemesFromExcel(excel_data, pfm_id)
                    all_schemes.extend(pfm_schemes)
                    successful_downloads += 1
                    self.logger.info(f"Successfully processed {pfm_id} with {len(pfm_schemes)} schemes")
                else:
                    self.logger.warning(f"No data found for {pfm_id}")
                    
            except Exception as e:
                self.logger.error(f"Error processing {pfm_id}: {str(e)}")
                continue
        
        self.logger.info(f"Total schemes collected: {len(all_schemes)} from {successful_downloads} PFMs")
        
        return {
            "data": all_schemes,
            "total_schemes": len(all_schemes),
            "successful_pfms": successful_downloads
        }
    
    def downloadNPSExcel(self, pfm_id, scheme_id):
        """
        Download Excel file for a specific PFM and scheme
        """
        url = f"https://npstrust.org.in/scheme-wise-nav-report-excel?navcatdataxls={pfm_id}&navyearselxls=12&navsubdataxls={scheme_id}"
        
        try:
            response = requests.get(url, timeout=30)
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
