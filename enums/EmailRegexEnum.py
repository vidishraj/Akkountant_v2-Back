from enum import Enum


# Regex to extract required information from the emails
class EmailRegexEnum(Enum):
    Millenia_Credit = r"(?P<card_number>\d{4}) for Rs (?P<amount_spent>[\d,]+(?:\.\d+)?) at (?P<merchant>.+?) on (" \
                      r"?P<transaction_date>[\d-]+) (?P<transaction_time>[\d:]+)\. Authorization code:- (" \
                      r"?P<authorization_code>\d+) "
    HDFC_DEBIT = r"Rs\.(?P<amount_spent>[\d,]+(?:\.\d+)?) has been debited from account \*\*(?P<account_number>\d{4}) "\
                 r"to VPA (?P<merchant>.+?) on (?P<transaction_date>[\d-]+)\. Your UPI transaction reference number " \
                 r"is (?P<upi_reference>\d+)\. "
    ICICI_AMAZON_PAY = r"Credit Card (?P<card_number>\w+) has been used for a transaction of INR (" \
                       r"?P<amount_spent>[\d,]+(?:\.\d+)?) on (?P<transaction_date>[\w\s\d," \
                       r"]+) at (?P<transaction_time>[\d:]+). Info: (?P<merchant>.+?)\. "
    YES_BANK_ACE = r"INR (?P<amount_spent>[\d,]+(?:\.\d+)?) has been spent on your YES BANK Credit Card ending with (" \
                   r"?P<card_number>\d{4}) at (?P<merchant>.+?) on (?P<transaction_date>[\d-]+) at (" \
                   r"?P<transaction_time>[" \
                   r"\d:]+ [apm]+)\. Avl Bal INR (?P<available_balance>[\d,]+\.\d{2}) "
