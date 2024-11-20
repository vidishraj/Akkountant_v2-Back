import logging
from abc import abstractmethod, ABC

import fitz

from utils.logger import Logger


class BaseParser(ABC):
    pagesInPDF: int
    password: str
    filePath: str
    _transactionList: []

    def __init__(self, name):
        self.password = None
        self._transactionList = []
        self.end_flag = False
        self.filePath = None
        self.logging = Logger(name).get_logger()

    def parseFile(self):
        try:
            self.countPages()
            self.readFirstPage()
            if self.pagesInPDF > 1:
                self.readMiddlePages()
                self.readLastPage()
        except Exception as ex:
            self.logging.error(f"Error has occurred while parsing file. Ending parse {ex}")
        finally:
            self.end_flag = True
            return self._transactionList

    @abstractmethod
    def readFirstPage(self):
        pass

    @abstractmethod
    def readMiddlePages(self):
        pass

    @abstractmethod
    def readLastPage(self):
        pass

    @abstractmethod
    def processTableOnPage(self, tables):
        pass

    def countPages(self):
        pdf = fitz.open(self.filePath)
        # If the file is encrypted and a password is provided, attempt to decrypt
        if pdf.needs_pass:
            if self.password:
                pdf.authenticate(self.password)
            else:
                raise ValueError("PDF is encrypted, and no password was provided.")

        self.pagesInPDF = pdf.page_count
        pdf.close()

    def setPath(self, path):
        self.filePath = path

    def setPassword(self, password):
        self.password = password
