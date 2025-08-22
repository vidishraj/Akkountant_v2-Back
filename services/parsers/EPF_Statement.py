from abc import ABC
from datetime import datetime
import tabula
import pandas as pd
from pandas import DataFrame

from services.parsers.Base_Parser import BaseParser


class EPFStatementParser(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)
        self.pending_contribution_row = None
        self.pending_interest_row = None

    def readFirstPage(self):
        # (top,left,bottom,right) - converted from LWTH: 38.44,532.6,219,577
        extraction_area = [219, 38.44, 796, 571.04]
        columns = [150, 232, 317, 402, 484]
        tables: [DataFrame] = tabula.io.read_pdf(
            self.filePath, area=extraction_area, guess=False,
            pages=1,
            columns=columns,
            stream=True, silent=True,
            password=self.password, pandas_options={'header': None})
        self.processTableOnPage(tables, page_num=1)

    def readMiddlePages(self):
        # Use entire page for middle pages
        extraction_area = [0, 38.44, 800, 571.04]
        columns = [150, 232, 317, 402, 484]
        for page_num in range(2, self.pagesInPDF):
            tables: [DataFrame] = tabula.io.read_pdf(
                self.filePath, area=extraction_area, guess=False,
                pages=page_num,
                columns=columns,
                stream=True, silent=True,
                password=self.password, pandas_options={'header': None})
            self.processTableOnPage(tables, page_num=page_num)

    def readLastPage(self):
        # Parse from top until 'Grand Total' is found
        extraction_area = [0, 38.44, 800, 571.04]
        columns = [150, 232, 317, 402, 484]
        tables: [DataFrame] = tabula.io.read_pdf(
            self.filePath, area=extraction_area, guess=False,
            pages=self.pagesInPDF,
            columns=columns,
            stream=True, silent=True,
            password=self.password, pandas_options={'header': None})
        self.processLastPageTable(tables, page_num=self.pagesInPDF)

    def processLastPageTable(self, tables, page_num):
        try:
            for index, table in enumerate(tables):
                prev_row = None
                
                # Check if we have a pending contribution from previous page
                if self.pending_contribution_row is not None:
                    # Check if first row is a date that matches the pending contribution
                    first_row = table.iloc[0] if len(table) > 0 else None
                    if first_row is not None and len(str(first_row.iloc[0]).strip()) == 6 and str(first_row.iloc[0]).strip().isdigit():
                        month_year = str(first_row.iloc[0]).strip()
                        month = month_year[:2]
                        year = month_year[2:]
                        date = f"{month}/{year}"
                        
                        emp_deposit = float(str(self.pending_contribution_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[1]) else 0
                        employer_deposit = float(str(self.pending_contribution_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[2]) else 0
                        
                        if emp_deposit > 0 or employer_deposit > 0:
                            self._transactionList.append({
                                'date': date,
                                'description': f'Contribution for {month}/{year}',
                                'employee_deposit': emp_deposit,
                                'employer_deposit': employer_deposit,
                                'amount': emp_deposit + employer_deposit
                            })
                    
                    self.pending_contribution_row = None
                
                # NOTE: Interest handling commented out - application calculates interest automatically
                # # Check if we have a pending interest from previous page
                # if self.pending_interest_row is not None:
                #     # Check if first row is a date that matches the pending interest
                #     first_row = table.iloc[0] if len(table) > 0 else None
                #     if first_row is not None and self.is_valid_date_format_ddmmyyyy(str(first_row.iloc[0]).strip()):
                #         date = str(first_row.iloc[0]).strip()
                #         
                #         emp_interest = float(str(self.pending_interest_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[1]) else 0
                #         employer_interest = float(str(self.pending_interest_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[2]) else 0
                #         
                #         if emp_interest > 0 or employer_interest > 0:
                #             self._transactionList.append({
                #                 'date': date,
                #                 'description': f'Interest Updated upto {date}',
                #                 'employee_interest': emp_interest,
                #                 'employer_interest': employer_interest,
                #                 'amount': emp_interest + employer_interest
                #             })
                #     
                #     self.pending_interest_row = None
                
                for innerIndex, row in table.iterrows():
                    # Stop processing when we hit 'Grand Total'
                    if 'Grand Total' in str(row.iloc[0]):
                        return
                    
                    # Look for contribution entries with month/year pattern (MMYYYY)
                    if len(str(row.iloc[0]).strip()) == 6 and str(row.iloc[0]).strip().isdigit():
                        month_year = str(row.iloc[0]).strip()
                        month = month_year[:2]
                        year = month_year[2:]
                        date = f"{month}/{year}"
                        
                        # Extract amounts from previous row (which contains "Cont. For Due-Month")
                        if prev_row is not None and 'Cont. For Due-Month' in str(prev_row.iloc[0]):
                            emp_deposit = float(str(prev_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[1]) else 0
                            employer_deposit = float(str(prev_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[2]) else 0
                            
                            if emp_deposit > 0 or employer_deposit > 0:
                                self._transactionList.append({
                                    'date': date,
                                    'description': f'Contribution for {month}/{year}',
                                    'employee_deposit': emp_deposit,
                                    'employer_deposit': employer_deposit,
                                    'amount': emp_deposit + employer_deposit
                                })
                        # Also check if we have a pending contribution from previous page
                        elif self.pending_contribution_row is not None:
                            emp_deposit = float(str(self.pending_contribution_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[1]) else 0
                            employer_deposit = float(str(self.pending_contribution_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[2]) else 0
                            
                            if emp_deposit > 0 or employer_deposit > 0:
                                self._transactionList.append({
                                    'date': date,
                                    'description': f'Contribution for {month}/{year}',
                                    'employee_deposit': emp_deposit,
                                    'employer_deposit': employer_deposit,
                                    'amount': emp_deposit + employer_deposit
                                })
                            self.pending_contribution_row = None
                    
                    # NOTE: Interest detection commented out - application calculates interest automatically
                    # # Look for interest entries with date pattern (DD/MM/YYYY)
                    # elif self.is_valid_date_format_ddmmyyyy(str(row.iloc[0]).strip()):
                    #     date = str(row.iloc[0]).strip()
                    #     
                    #     # Extract amounts from previous row (which contains "Int. Updated upto")
                    #     if prev_row is not None and 'Int. Updated upto' in str(prev_row.iloc[0]):
                    #         emp_interest = float(str(prev_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[1]) else 0
                    #         employer_interest = float(str(prev_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[2]) else 0
                    #         
                    #         if emp_interest > 0 or employer_interest > 0:
                    #             self._transactionList.append({
                    #                 'date': date,
                    #                 'description': f'Interest Updated upto {date}',
                    #                 'employee_interest': emp_interest,
                    #                 'employer_interest': employer_interest,
                    #                 'amount': emp_interest + employer_interest
                    #             })
                    #     # Also check if we have a pending interest from previous page
                    #     elif self.pending_interest_row is not None:
                    #         emp_interest = float(str(self.pending_interest_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[1]) else 0
                    #         employer_interest = float(str(self.pending_interest_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[2]) else 0
                    #         
                    #         if emp_interest > 0 or employer_interest > 0:
                    #             self._transactionList.append({
                    #                 'date': date,
                    #                 'description': f'Interest Updated upto {date}',
                    #                 'employee_interest': emp_interest,
                    #                 'employer_interest': employer_interest,
                    #                 'amount': emp_interest + employer_interest
                    #             })
                    #         self.pending_interest_row = None
                    
                    prev_row = row
        except Exception as ex:
            self.logging.info(f"{ex}")

    @staticmethod
    def is_valid_date_format(date_str: str) -> bool:
        """
        Validates if the given date string is in the format 'MMM-YYYY' (e.g., 'Dec-2022').
        Args:
            date_str (str): The date string to validate.
        Returns:
            bool: True if the date is valid, False otherwise.
        """
        try:
            # Parse the date with the specified format
            datetime.strptime(date_str, "%b-%Y")
            return True
        except ValueError:
            return False
        except Exception:
            return False
    
    @staticmethod
    def is_valid_date_format_ddmmyyyy(date_str: str) -> bool:
        """
        Validates if the given date string is in the format 'DD/MM/YYYY' (e.g., '31/03/2024').
        Args:
            date_str (str): The date string to validate.
        Returns:
            bool: True if the date is valid, False otherwise.
        """
        try:
            # Parse the date with the specified format
            datetime.strptime(date_str, "%d/%m/%Y")
            return True
        except ValueError:
            return False
        except Exception:
            return False

    def processTableOnPage(self, tables, page_num=1):
        try:
            for index, table in enumerate(tables):
                prev_row = None
                
                # Check if we have a pending contribution from previous page
                if self.pending_contribution_row is not None:
                    # Check if first row is a date that matches the pending contribution
                    first_row = table.iloc[0] if len(table) > 0 else None
                    if first_row is not None and len(str(first_row.iloc[0]).strip()) == 6 and str(first_row.iloc[0]).strip().isdigit():
                        month_year = str(first_row.iloc[0]).strip()
                        month = month_year[:2]
                        year = month_year[2:]
                        date = f"{month}/{year}"
                        
                        emp_deposit = float(str(self.pending_contribution_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[1]) else 0
                        employer_deposit = float(str(self.pending_contribution_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[2]) else 0
                        
                        if emp_deposit > 0 or employer_deposit > 0:
                            self._transactionList.append({
                                'date': date,
                                'description': f'Contribution for {month}/{year}',
                                'employee_deposit': emp_deposit,
                                'employer_deposit': employer_deposit,
                                'amount': emp_deposit + employer_deposit
                            })
                    
                    self.pending_contribution_row = None
                
                # NOTE: Interest handling commented out - application calculates interest automatically
                # # Check if we have a pending interest from previous page
                # if self.pending_interest_row is not None:
                #     # Check if first row is a date that matches the pending interest
                #     first_row = table.iloc[0] if len(table) > 0 else None
                #     if first_row is not None and self.is_valid_date_format_ddmmyyyy(str(first_row.iloc[0]).strip()):
                #         date = str(first_row.iloc[0]).strip()
                #         
                #         emp_interest = float(str(self.pending_interest_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[1]) else 0
                #         employer_interest = float(str(self.pending_interest_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[2]) else 0
                #         
                #         if emp_interest > 0 or employer_interest > 0:
                #             self._transactionList.append({
                #                 'date': date,
                #                 'description': f'Interest Updated upto {date}',
                #                 'employee_interest': emp_interest,
                #                 'employer_interest': employer_interest,
                #                 'amount': emp_interest + employer_interest
                #             })
                #     
                #     self.pending_interest_row = None
                
                for innerIndex, row in table.iterrows():
                    # Look for contribution entries with month/year pattern (MMYYYY)
                    if len(str(row.iloc[0]).strip()) == 6 and str(row.iloc[0]).strip().isdigit():
                        month_year = str(row.iloc[0]).strip()
                        month = month_year[:2]
                        year = month_year[2:]
                        date = f"{month}/{year}"
                        
                        # Extract amounts from previous row (which contains "Cont. For Due-Month")
                        if prev_row is not None and 'Cont. For Due-Month' in str(prev_row.iloc[0]):
                            emp_deposit = float(str(prev_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[1]) else 0
                            employer_deposit = float(str(prev_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[2]) else 0
                            
                            if emp_deposit > 0 or employer_deposit > 0:
                                self._transactionList.append({
                                    'date': date,
                                    'description': f'Contribution for {month}/{year}',
                                    'employee_deposit': emp_deposit,
                                    'employer_deposit': employer_deposit,
                                    'amount': emp_deposit + employer_deposit
                                })
                        # Also check if we have a pending contribution from previous page
                        elif self.pending_contribution_row is not None:
                            emp_deposit = float(str(self.pending_contribution_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[1]) else 0
                            employer_deposit = float(str(self.pending_contribution_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_contribution_row.iloc[2]) else 0
                            
                            if emp_deposit > 0 or employer_deposit > 0:
                                self._transactionList.append({
                                    'date': date,
                                    'description': f'Contribution for {month}/{year}',
                                    'employee_deposit': emp_deposit,
                                    'employer_deposit': employer_deposit,
                                    'amount': emp_deposit + employer_deposit
                                })
                            self.pending_contribution_row = None
                    
                    # NOTE: Interest detection commented out - application calculates interest automatically
                    # # Look for interest entries with date pattern (DD/MM/YYYY)
                    # elif self.is_valid_date_format_ddmmyyyy(str(row.iloc[0]).strip()):
                    #     date = str(row.iloc[0]).strip()
                    #     
                    #     # Extract amounts from previous row (which contains "Int. Updated upto")
                    #     if prev_row is not None and 'Int. Updated upto' in str(prev_row.iloc[0]):
                    #         emp_interest = float(str(prev_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[1]) else 0
                    #         employer_interest = float(str(prev_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(prev_row.iloc[2]) else 0
                    #         
                    #         if emp_interest > 0 or employer_interest > 0:
                    #             self._transactionList.append({
                    #                 'date': date,
                    #                 'description': f'Interest Updated upto {date}',
                    #                 'employee_interest': emp_interest,
                    #                 'employer_interest': employer_interest,
                    #                 'amount': emp_interest + employer_interest
                    #             })
                    #     # Also check if we have a pending interest from previous page
                    #     elif self.pending_interest_row is not None:
                    #         emp_interest = float(str(self.pending_interest_row.iloc[1]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[1]) else 0
                    #         employer_interest = float(str(self.pending_interest_row.iloc[2]).replace(',', '').replace('NaN', '0')) if pd.notna(self.pending_interest_row.iloc[2]) else 0
                    #         
                    #         if emp_interest > 0 or employer_interest > 0:
                    #             self._transactionList.append({
                    #                 'date': date,
                    #                 'description': f'Interest Updated upto {date}',
                    #                 'employee_interest': emp_interest,
                    #                 'employer_interest': employer_interest,
                    #                 'amount': emp_interest + employer_interest
                    #             })
                    #         self.pending_interest_row = None
                    
                    prev_row = row
                
                # Check if last row is a contribution that might continue on next page
                if prev_row is not None and 'Cont. For Due-Month' in str(prev_row.iloc[0]):
                    self.pending_contribution_row = prev_row
                # NOTE: Interest row detection commented out - application calculates interest automatically
                # # Check if last row is an interest that might continue on next page
                # elif prev_row is not None and 'Int. Updated upto' in str(prev_row.iloc[0]):
                #     self.pending_interest_row = prev_row
                    
        except Exception as ex:
            self.logging.info(f"{ex}")