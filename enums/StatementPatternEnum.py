from enum import Enum


# Regex to search for emails with statements
class StatementPatternEnum(Enum):
    Millenia_Credit = "Millennia Credit Card Statement "
    HDFC_DEBIT = "from:(hdfcbanksmartstatement@hdfcbank.net) "
    ICICI_AMAZON_PAY = "Amazon Pay ICICI Bank Credit Card Statement "
    YES_BANK_ACE = "from:(estatement@yesbank.in) Credit Card, ACE "
    YES_BANK_DEBIT = "from:(estatement@yesbank.in) -{ACE, Credit Card} "

