from enum import Enum


class PatternEnum(Enum):
    Millenia_Credit = "subject:(Alert : Update on your HDFC Bank Credit Card)"
    HDFC_DEBIT = "You have done a UPI txn. Check details!"
    YES_BANK_DEBIT = "aonfajngjkgndkjgndkdnsnaogjnaognagoajgaogi"
    ICICI_AMAZON_PAY = "from:(credit_cards@icicibank.com) Transaction alert for your ICICI Bank Credit Card"
    YES_BANK_ACE = "Yes Bank - Transaction Alert"
