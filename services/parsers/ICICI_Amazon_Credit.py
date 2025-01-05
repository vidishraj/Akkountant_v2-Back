from abc import ABC

import pandas
import tabula
import re

from services.parsers.Base_Parser import BaseParser
from utils.GenericUtils import GenericUtil


class ICICICreditCardStatementParser(BaseParser, ABC):
    preDefinedColumns = ['Date', 'SerNo.', 'Transaction Details', 'Reward', 'Intl.#',
                         'Amount (in`)']

    def __init__(self):
        super().__init__(name=__name__)

    def readFirstPage(self):
        extraction_area = [365, 199, 623, 568]
        columns = [243, 299, 435, 473, 515, 568]
        tables: [pandas.core.frame.DataFrame] = tabula.io.read_pdf(
            self.filePath,
            pages='1', area=extraction_area, guess=False,
            stream=True, silent=True,
            columns=columns, password=self.password)
        self.processTableOnPage(tables)

    def readMiddlePages(self):
        extraction_area = [58, 30, 834, 589]
        columns = [86, 169, 366, 437, 507, 562]
        tables: [pandas.core.frame.DataFrame] = tabula.io.read_pdf(
            self.filePath,
            pages='2', area=extraction_area, guess=False,
            stream=True, silent=True,
            columns=columns, password=self.password)
        if tables[0].columns[0] == "Date":
            tables[0].columns = self.preDefinedColumns
            self.processTableOnPage(tables)

    def readLastPage(self):
        return

    def processTableOnPage(self, tables):
        dateRegex: str = "^(0[1-9]|1\\d|2\\d|3[01])\\/(0[1-9]|1[0-2])\\/(19|20)\\d{2}$"
        for index, table in enumerate(tables):
            for innerIndex, rowDate in enumerate(table['Date']):
                if re.match(dateRegex, str(rowDate)):
                    amountConverted = str(table['Amount (in`)'][innerIndex]).replace(",", "")
                    if "CR" in amountConverted:
                        amountConverted = amountConverted.replace("CR", "")
                        amountConverted = -1 * float(amountConverted)
                    else:
                        amountConverted = float(amountConverted)

                    date = table['Date'][innerIndex]
                    desc = table['Transaction Details'][innerIndex]
                    amount = amountConverted

                    reference = GenericUtil().generate_reference_id(date, desc, amount)
                    self._transactionList.append({
                        'date': date,
                        'reference': reference,
                        'amount': amount,
                        'description': desc
                    })
