from abc import ABC
from datetime import datetime
import tabula
from pandas import DataFrame

from services.parsers.Base_Parser import BaseParser


class EPFStatementParser(BaseParser, ABC):

    def __init__(self):
        super().__init__(name=__name__)

    def readFirstPage(self):
        # (top,left,bottom,right)
        extraction_area = [222, 25, 520, 560]
        columns = [82, 138, 168, 283, 339, 397, 453, 515]
        tables: [DataFrame] = tabula.read_pdf(
            self.filePath, area=extraction_area, guess=False,
            pages=1,
            columns=columns,
            stream=True, silent=True,
            password=self.password, pandas_options={'header': None})
        self.processTableOnPage(tables)

    def readMiddlePages(self):
        return

    def readLastPage(self):
        return

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

    def processTableOnPage(self, tables):
        try:
            # pandas.set_option('display.max_columns', 7)   #For debugging only.
            for index, table in enumerate(tables):
                for innerIndex, row in table.iterrows():
                    if self.is_valid_date_format(row.iloc[0]):
                        self._transactionList.append({
                            'date': row.iloc[1],
                            'description': row.iloc[3],
                            'amount': float(row.iloc[6].replace(',', '')) + float(row.iloc[7].replace(',', ''))
                        })
        except Exception as ex:
            self.logging.info(f"{ex}")