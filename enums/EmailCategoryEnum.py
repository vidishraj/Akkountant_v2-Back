from enum import Enum


class EmailCategory(Enum):
    TRANSACTION_ALERT = "transaction_alert"
    BANK_STATEMENT = "bank_statement"
    INVESTMENT_CONFIRMATION = "investment_confirmation"
    EPF_PASSBOOK = "epf_passbook"
    GOLD_RECEIPT = "gold_receipt"
    FREELANCE_PAYMENT = "freelance_payment"
    UNKNOWN = "unknown"
