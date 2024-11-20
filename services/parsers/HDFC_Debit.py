import math
import re
from abc import ABC
import pandas
import tabula

from services.parsers.Base_Parser import BaseParser
from utils.GenericUtils import GenericUtil


class HDFCDebitParser(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)

    def readFirstPage(self):
        # (top,left,bottom,right)
        extraction_area = [266, 8, 800, 765]
        try:
            tables: [pandas.core.frame.DataFrame] = tabula.read_pdf(
                self.filePath, area=extraction_area, guess=False,
                pages="all",
                lattice=True, silent=True,
                password=self.password, pandas_options={'header': None})
            self.processTableOnPage(tables)
        except:
            extraction_area = [228, 27, 800, 700]
            columns = [67, 272, 357, 397, 475, 551, 700]
            tables: [pandas.core.frame.DataFrame] = tabula.read_pdf(
                self.filePath, area=extraction_area, guess=False,
                pages="all",
                stream=True, silent=True,
                pandas_options={'header': None}, columns=columns)
            self.processTableOnPageV2(tables)

    def processTableOnPageV2(self, tables: [pandas.core.frame.DataFrame]):
        dateRegex: str = r'\b(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{2}\b'
        try:
            # pandas.set_option('display.max_columns', 7)   #For debugging only.
            for index, table in enumerate(tables):
                for innerIndex, row in table.iterrows():
                    try:
                        date = row[0]
                        if re.match(dateRegex, str(date)):
                            date = self.format_date(date)
                            desc = row[1]
                            amount = None
                            if type(row[4]) != str and math.isnan(row[4]):
                                amount = -1 * float(str(row[5]).replace(",", ''))
                            if type(row[5]) != str and math.isnan(row[5]):
                                amount = float(str(row[4]).replace(",", ''))
                            closingBalance = float(str(row[6]).replace(",", ''))
                            reference = GenericUtil().generate_reference_id(date, desc, amount)
                            self._transactionList.append(
                                {
                                    'reference': reference,
                                    'date': date,
                                    'description': desc,
                                    'amount': amount
                                })
                        elif type(row[0]) != str and type(row[2]) != str and math.isnan(row[0]) and type(
                                row[1]) == str and math.isnan(row[2]) and len(self._transactionList) > 0:
                            lastTuple = self._transactionList[-1]
                            lastList = list(lastTuple)
                            lastList[1] += row[1]
                            self._transactionList[-1] = tuple(lastList)
                    except Exception as ex:
                        self.logging.error(f"Error {ex}")
                        continue

                self.logging.info(f"Finished table {index + 1}. Transactions analysed: {len(self._transactionList)}")
            self.logging.info(f"Total transactions {len(self._transactionList)}")
        except Exception as ex:
            self.logging.error(f"Error reading tables in v2 as well. {ex}")

    def processTableOnPage(self, tables: [pandas.core.frame.DataFrame]):
        dateRegex: str = "^(0[1-9]|1\\d|2\\d|3[01])\\/(0[1-9]|1[0-2])\\/(19|20)\\d{2}$"

        try:
            for index, table in enumerate(tables):
                for innerIndex, row in table.iterrows():
                    date = row[0]
                    if re.match(dateRegex, str(date)):
                        date = self.format_date(date)
                        desc = row[1]
                        amount = str(row[4])
                        amount = amount.replace(",", "")
                        closingAmount = row[6]
                        if float(amount) == 0:
                            cleanVal = str(row[5]).replace(',', '')
                            creditVal = float(cleanVal)
                            amount = -1 * creditVal
                        reference = GenericUtil().generate_reference_id(date, desc, amount)
                        self._transactionList.append(
                            {
                                'reference': reference,
                                'date': date,
                                'description': desc,
                                'amount': amount
                            })
        except Exception as ex:
            self.logging.error(f"Error occurred while inserting statement to HDFC.{ex}")
            return

    def readLastPage(self):
        return

    def readMiddlePages(self):
        return


    @staticmethod
    def format_date(input_date):
        # To have a common date format. Hopefully no statements with dates from 1900's or 2100's lol
        day, month, year = input_date.split('/')
        if len(year) == 2:
            year = '20' + year

        return f"{day}/{month}/{year}"