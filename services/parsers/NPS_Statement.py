import math
import re
from abc import ABC

import pandas
import tabula

from services.parsers.Base_Parser import BaseParser


class NPSParser(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)
        self.firstPageFormatTables = []
        self.secondPageFormatTables = []
        self.nameList = []

    def readLastPage(self):
        self.readSecurityNamesTier1()
        self.processTableOnPage(self.firstPageFormatTables[0])
        self.processTableOnPage(self.secondPageFormatTables[0])

    def readMiddlePages(self):
        # (top, left, bottom, right).
        # Middle page tables is a little smaller.
        columns = [250, 291, 329, 371, 410, 452, 495]

        tables: [pandas.DataFrame] = tabula.io.read_pdf(
            self.filePath,
            pages=f'2-{self.pagesInPDF}', guess=False, pandas_options={'header': None},
            stream=True, silent=True,
            columns=columns, multiple_tables=True)
        self.secondPageFormatTables.append(tables)

    def readFirstPage(self):
        # (top, left, bottom, right).
        extraction_area = [507, 8, 800, 581]
        columns = [237, 281, 320, 362, 402, 447, 493]

        tables: [pandas.DataFrame] = tabula.io.read_pdf(
            self.filePath,
            pages='all', guess=False, pandas_options={'header': None},
            stream=True, silent=True,
            columns=columns, multiple_tables=True)
        self.firstPageFormatTables.append(tables)

    def processTableOnPage(self, tables: [pandas.DataFrame]):
        startParsing = False
        nameRow = 0
        name = ""
        nav = None
        quantity = None
        for index, table in enumerate(tables):
            for row_index, row in table.iterrows():
                if startParsing:
                    if name.strip() == self.nameList[nameRow]:
                        nameRow += 1
                        self._transactionList.append({
                            'name': name.strip(),
                            'nav': nav,
                            'quantity': quantity
                        })
                        name = ''
                        nav = None
                        quantity = None
                    if self.isAValidLine(row):
                        name += f"{row.iloc[0].strip()} "
                        nav = row.iloc[4]
                        quantity = row.iloc[1]
                    elif self.isNameOverFlow(row) and row.iloc[0] != 'Note':
                        name += f"{row.iloc[0].strip()} "

                if row.iloc[0] == "Scheme Name" and row.iloc[1] == "TotalUnits" and row.iloc[2] == "BlockedUnits":
                    startParsing = True
        # Changing nameList to only contain remaining names
        self.nameList = self.nameList[nameRow:len(self.nameList)]

    def isAValidLine(self, row):
        """
        If the row is valid, then we check the types of all indicies
        :param row:The row being iterated
        :return: True or False based on checks
        """
        try:
            if len(row) < 8:
                return False
            for i in range(0, 8):
                if not isinstance(row.iloc[i], str) and math.isnan(row.iloc[i]):
                    return False
            floatList = []
            if not isinstance(row.iloc[0], str):
                return False
            for i in range(1, 8):
                floatList.append(float(row.iloc[i].replace(',', '')))
            for item in floatList:
                if not isinstance(item, float):
                    return False
            return True
        except Exception as ex:
            self.logging.error(f"Error while parsing line in NPS statement {ex}")
            return False

    @staticmethod
    def isNameOverFlow(row):
        try:
            if len(row) < 8:
                # At least 8 items should be there
                return False
            if not isinstance(row.iloc[0], str):
                return False
            for i in range(1, 8):
                if not math.isnan(row.iloc[i]):
                    return False
            return True
        except:
            return False

    def readSecurityNamesTier1(self):
        # (top, left, bottom, right).
        extraction_area = [100, 8, 800, 581]
        columns = [129, 493]

        percentagePattern = r"^\d{1,3}(\.\d{2})?%$"
        tables: [pandas.DataFrame] = tabula.io.read_pdf(
            self.filePath,
            area=extraction_area,
            pages='all', guess=False, pandas_options={'header': None},
            stream=True, silent=True,
            columns=columns, multiple_tables=True)
        for index, table in enumerate(tables):
            for row_index, row in table.iterrows():
                if isinstance(row.iloc[2], str) and re.match(percentagePattern, row.iloc[2]):
                    self.nameList.append(row.iloc[1])
