from enum import Enum


# Enum to parse date correctly
class DateStatementEnum(Enum):
    Millenia_Credit = "%d/%m/%Y"
    HDFC_DEBIT = "%d/%m/%Y"
    ICICI_AMAZON_PAY = "%d/%m/%Y"
    YES_BANK_ACE = "%d/%m/%Y"
    YES_BANK_DEBIT = "%d/%m/%Y"

