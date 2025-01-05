import re
from abc import ABC

import pandas
import tabula

from services.parsers.Base_Parser import BaseParser
from utils.GenericUtils import GenericUtil


class HDFCMilleniaParse(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)

    def readFirstPage(self):
        extraction_area = [429, 23, 677, 588]
        columns = [104, 476, 677]
        try:
            tables: [pandas.core.frame.DataFrame] = tabula.io.read_pdf(
                self.filePath,
                pages='1', area=extraction_area, guess=False,
                stream=True, silent=True,
                columns=columns, password=self.password)
        except:
            tables: [pandas.core.frame.DataFrame] = tabula.io.read_pdf(
                self.filePath,
                pages='1', area=extraction_area, guess=False,
                stream=True, silent=True,
                columns=columns)
        self.processTableOnPage(tables)


    def readMiddlePages(self):
        extraction_area = [64, 23, 709, 588]
        columns = [104, 476, 677]
        for i in range(2, self.pagesInPDF):
            try:
                tables: [pandas.core.frame.DataFrame] = tabula.io.read_pdf(
                    self.filePath,
                    pages=i, area=extraction_area, guess=False,
                    stream=True, silent=True,
                    columns=columns, password=self.password)
            except:
                tables: [pandas.core.frame.DataFrame] = tabula.io.read_pdf(
                    self.filePath,
                    pages=i, area=extraction_area, guess=False,
                    stream=True, silent=True,
                    columns=columns)
            self.processTableOnPage(tables)

    def readLastPage(self):
        # No need to implement now
        return

    def processTableOnPage(self, tables):
        dateRegex: str = "^(0[1-9]|1\\d|2\\d|3[01])\\/(0[1-9]|1[0-2])\\/(19|20)\\d{2}$"
        for index, table in enumerate(tables):
            for innerIndex, rowDate in enumerate(table['Date']):
                if re.match(dateRegex, str(rowDate)):
                    amountConverted = str(table['Amount (in Rs.)'][innerIndex]).replace(",", "")
                    if "Cr" in amountConverted:
                        amountConverted = amountConverted.replace("Cr", "")
                        amountConverted = -1 * float(amountConverted)
                    else:
                        amountConverted = float(amountConverted)
                    date = table['Date'][innerIndex]
                    desc = table['Transaction Description'][innerIndex]
                    amount = amountConverted
                    self._transactionList.append({
                        "reference": GenericUtil().generate_reference_id(date, desc, amount),
                        'date': date,
                        'description': table['Transaction Description'][innerIndex],
                        'amount': amountConverted})
