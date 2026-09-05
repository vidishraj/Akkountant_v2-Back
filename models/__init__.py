from models.fileDetails import FileDetails
from models.users import User
from models.transactions import Transactions
# ak-ex2-v2: snapshot-before-delete audit table for the non-savings
# row stripper. Import BEFORE db.create_all() so the fresh table is
# created on next boot. No ALTER TABLE prereq (new table, not a
# column addition).
from models.strippedTransactionsAudit import StrippedTransactionsAudit
from models.transactionsForReview import TransactionForReview
from models.savedTags import SavedTags
from models.statementPasswords import StatementPasswords
from models.googleTokens import UserToken
from models.securities import SoldSecurities
from models.depositSecurities import DepositSecurities
from models.purchasedSecurities import PurchasedSecurities
from models.GoldDetails import GoldDetails
from models.securityTransactions import SecurityTransactions
from models.Jobs import Job
from models.foTrades import FOTrade
from models.userPersonalInfo import UserPersonalInfo
from models.processedEmails import ProcessedEmails
from models.statementPeriods import StatementPeriod
from models.transferLinks import TransferLink
from models.investmentSnapshot import InvestmentSnapshot
from models.Base import Base
from .freelance_management import Customer, InvoiceTemplate, Invoice, InvoiceItem, Signature, InvoiceSignature, InvoicePayment, InvoiceCustomField, CurrencyEnum, InvoiceStatusEnum, PendingPaymentClaim
from .customer_emails import CustomerEmail
from .stockTrade import TradeAssociation
from .user_files import UserFolder, UserFile
from .jobEmails import JobEmail
from .jobProcessedEmails import ProcessedEmail as JobProcessedEmail
from .portfolioVisitors import PortfolioVisitor
# ak-bq5: cross-device agent chat persistence — see dispatch hq-wisp-rr15b.
# Order: AgentConversation first so AgentMessage's FK target exists at
# create_all() time. Both must be imported BEFORE db.create_all() runs
# in app.py so the schema is materialized.
from .AgentConversation import AgentConversation
from .AgentMessage import AgentMessage, AGENT_MESSAGE_ROLES
# ak-9dz: agent-produced file downloads. Standalone table (no ALTER
# on existing). Import order matters: AgentConversation + AgentMessage
# must precede this so FK targets exist at create_all() time.
from .agentFileAttachments import AgentFileAttachment
