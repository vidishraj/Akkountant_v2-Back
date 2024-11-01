from enum import Enum


class PatternEnum(Enum):
    Millenia_Credit = "subject:(Alert : Update on your HDFC Bank Credit Card)"
    HDFC_DEBIT = "You have done a UPI txn. Check details!"
    ICICI_AMAZON_PAY = "from:(credit_cards@icicibank.com) Transaction alert for your ICICI Bank Credit Card"
    YES_BANK = "Yes Bank - Transaction Alert"
