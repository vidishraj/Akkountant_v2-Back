import re
from abc import ABC

import pandas
import tabula

from services.parsers.Base_Parser import BaseParser
from utils.GenericUtils import GenericUtil


class YESBankCreditParser(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)

    def readFirstPage(self):
        tables = []
        extraction_area = [10, 20, 800, 650]
        columns = [96, 485, 650]
        for i in range(1, 1000):
            try:
                tables += tabula.read_pdf(
                    self.filePath, guess=True, silent=True,
                    area=extraction_area, stream=True,
                    pages=f'{i}',
                    password=self.password, pandas_options={'header': None}, columns=columns, multiple_tables=True)
            except:
                break
        self.processTableOnPage(tables)

    def processTableOnPage(self, tables: [pandas.core.frame.DataFrame]):
        dateRegex: str = r'\b(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/\d{4}\b'
        try:
            # pandas.set_option('display.max_columns', 7)   #For debugging only.
            tableStart = False
            for index, table in enumerate(tables):
                for innerIndex, row in table.iterrows():
                    try:
                        if str(row[0]).strip() == "Date" and str(row[1]).strip() == "Transaction Details" and str(
                                row[2]).strip() == "Amount (Rs.)":
                            tableStart = True

                        date = row[0]
                        if tableStart and "End of the statement" in row[1]:
                            return
                        if tableStart and re.match(dateRegex, date):
                            desc = str(row[1])
                            amount = row[2]
                            descSplit = desc.split("Ref No:")
                            if len(descSplit) > 1:
                                desc = descSplit[0][0:-2]
                            cleanedAmount = float(str(amount[0:-3]).replace(",", ''))
                            amountType = amount[len(amount) - 2: len(amount)]
                            if amountType == "Cr":
                                cleanedAmount = -1 * cleanedAmount
                            self._transactionList.append(
                                {'reference': GenericUtil().generate_reference_id(date, desc, cleanedAmount),
                                 'date': date, 'description': desc, 'amount': cleanedAmount})
                    except Exception as ex:
                        self.logging.error(f"Error {ex}")
                        continue
        except Exception as ex:
            self.logging.error(f"Error reading tables in YES bank Credit {ex}")

    def readMiddlePages(self):
        return

    def readLastPage(self):
        return
