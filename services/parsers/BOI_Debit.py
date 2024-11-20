from abc import ABC
import pandas
import tabula
from services.parsers.Base_Parser import BaseParser
from utils.GenericUtils import GenericUtil


class BOIDebitParser(BaseParser, ABC):
    month_mapping = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12"
    }

    def __init__(self):
        super().__init__(name=__name__)

    def processTableOnPage(self, tables):
        for index, item in enumerate(tables['Transaction']):
            if type(item) == str:
                itemList: list = item.split('-')
                if len(itemList) == 3:
                    itemList[1] = self.month_mapping[itemList[1]]
                    newDateFormat = "/".join(itemList)
                    if tables['Debit'][index] != "-":
                        amount = float(tables['Debit'][index])
                    else:
                        amount = -1 * float(tables['Credit'][index])
                    description = tables['Narration'][index]
                    self._transactionList.append(
                        {"reference": GenericUtil().generate_reference_id(newDateFormat, description, amount),
                         "date": newDateFormat,
                         "description": description,
                         'amount': amount})

    def readFirstPage(self):
        extraction_area = [244, 59, 796, 541]
        columns = [116, 188, 363, 413, 466, 537]
        tables: [pandas.core.frame.DataFrame] = tabula.read_pdf(
            self.filePath,
            pages='1', area=extraction_area, guess=False,
            stream=True, silent=True,
            columns=columns, password=self.password)
        self.processTableOnPage(tables[0])

    def readMiddlePages(self):
        extraction_area = [1, 59, 1000, 541]
        columns = [116, 188, 363, 413, 466, 537]
        tables: [pandas.core.frame.DataFrame] = tabula.read_pdf(
            self.filePath,
            pages=f'2-{self.pagesInPDF}', area=extraction_area, guess=True,
            stream=True, silent=True,
            columns=columns, password=self.password, pandas_options={'header': None})
        newColumns = ['Transaction', 'Instrument Id', 'Narration', 'Debit', 'Credit', 'Balance']
        for index, table in enumerate(tables):
            tables[index].columns = newColumns
            self.processTableOnPage(tables[index])
        tables[0].columns = newColumns

    def readLastPage(self):
        return
