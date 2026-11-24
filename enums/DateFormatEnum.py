from enum import Enum


# Enum to parse date correctly
class DateStatementEnum(Enum):
    Millenia_Credit = "%d/%m/%Y"
    HDFC_DEBIT = "%d/%m/%Y"
    ICICI_AMAZON_PAY = "%d/%m/%Y"
    YES_BANK_ACE = "%d/%m/%Y"
    YES_BANK_DEBIT = "%d/%m/%Y"
    BOI = "%d/%m/%Y"
    HDFC_REGALIA = "%d/%m/%Y"
    EPF_STATEMENT = "%d-%m-%Y"
    # ak-iu8i (2026-11-24): every bank id the mail pipeline can produce
    # needs an entry. Without one, convert_to_sql_datetime raised
    # AttributeError on every row (SBI / Axis / Kotak / BoB / Paytm
    # statements landed 0 rows). The format is the extraction CONTRACT,
    # not the bank's printed layout: the PDF statement prompts instruct
    # the agent to emit `date: "DD/MM/YYYY"` (mailProcessorService), the
    # same as every entry above.
    AXIS = "%d/%m/%Y"
    KOTAK = "%d/%m/%Y"
    SBI = "%d/%m/%Y"
    SBI_CARD = "%d/%m/%Y"
    BOB = "%d/%m/%Y"
    PAYTM = "%d/%m/%Y"

