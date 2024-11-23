import math
import re
from abc import ABC

import pandas
import tabula

from services.parsers.Base_Parser import BaseParser
from utils.GenericUtils import GenericUtil


class YESBankDebitParser(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)

    def readFirstPage(self):
        # (top,left,bottom,right)
        columns = [92, 144, 282, 354, 420, 482, 700]
        extraction_area = [250, 44, 800, 700]
        try:
            tables: [pandas.core.frame.DataFrame] = tabula.read_pdf(
                self.filePath, guess=True,
                area=extraction_area, stream=True,
                pages="all", silent=True,
                password=self.password, pandas_options={'header': None}, columns=columns, multiple_tables=True)
            self.processTableOnPage(tables)
        except Exception as ex:
            self.logging.error(f"Error finding tables in statement Yes Bank Debit. {ex}")

    def processTableOnPage(self, tables: [pandas.core.frame.DataFrame]):
        dateRegex: str = r'\b(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}\b'
        try:
            # pandas.set_option('display.max_columns', 7)   #For debugging only.
            firstColP1 = False
            firstColP2 = False
            for index, table in enumerate(tables):
                for innerIndex, row in table.iterrows():
                    try:
                        if str(row[0]).strip() == "Transaction":
                            firstColP1 = True
                        elif firstColP1 and str(row[0]).strip() == "Date":
                            firstColP2 = True
                        if str(row[0]).strip() == "Opening Ba":
                            return
                        date = row[0]
                        if firstColP1 and firstColP2 and re.match(dateRegex, str(date).strip()):
                            desc = row[2]
                            amount = 0.0
                            if type(row[4]) == str and type(row[5]) == str:
                                crAmount = float(str(row[5]).replace(",", ''))
                                dbAmount = float(str(row[4]).replace(",", ''))
                                if dbAmount == 0.0:
                                    amount = -1 * crAmount
                                else:
                                    amount = dbAmount
                            if type(desc) != str and math.isnan(desc):
                                if innerIndex - 1 > 0 and type(table.iloc[innerIndex - 1][2]) == str:
                                    desc = table.iloc[innerIndex - 1][2]
                                if innerIndex + 1 < len(table) and type(table.iloc[innerIndex + 1][2]) == str:
                                    desc += table.iloc[innerIndex + 1][2]
                            self._transactionList.append(
                                {'reference': GenericUtil().generate_reference_id(date, desc, amount),
                                 'date': date,
                                 'description': desc,
                                 'amount': amount
                                 })
                    except Exception as ex:
                        self.logging.error(f"Error {ex}")
                        continue
        except Exception as ex:
            self.logging.error(f"Error reading tables in Yes Bank Debit. {ex}")

    def readMiddlePages(self):
        return

    def readLastPage(self):
        return