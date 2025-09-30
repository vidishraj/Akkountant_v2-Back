import re
from typing import Dict, List, Tuple, Optional
from utils.logger import Logger


class EmailClassifier:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EmailClassifier, cls).__new__(cls)
            cls.logger = Logger("email_classifier").get_logger()
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self._init_patterns()
            self.initialized = True

    def _init_patterns(self):
        # Map domains to specific bank enum values
        self.bank_domains = {
            'hdfcbank.net': ['Millenia_Credit', 'HDFC_DEBIT'],  # HDFC domains map to multiple enum values
            'hdfcbank.com': ['Millenia_Credit', 'HDFC_DEBIT'],
            'icicibank.com': ['ICICI_AMAZON_PAY'],
            'icicibank.co.in': ['ICICI_AMAZON_PAY'],
            'yesbank.in': ['YES_BANK_DEBIT', 'YES_BANK_ACE'],
            'yesbank.co.in': ['YES_BANK_DEBIT', 'YES_BANK_ACE'],
            'bankofindia.co.in': ['BOI'],
            'bankofindia.com': ['BOI']
        }

        self.transaction_alert_patterns = {
            'subject_keywords': [
                'transaction alert', 'alert', 'update on your', 'account update',
                'upi txn', 'payment alert', 'debit alert', 'credit alert',
                'transaction notification', 'payment notification'
            ],
            'sender_keywords': [
                'alerts@', 'instaalerts', 'notifications@', 'credit_cards@'
            ],
            'content_indicators': [
                r'rs\.?\s*[\d,]+', r'₹\s*[\d,]+', r'inr\s*[\d,]+',
                r'amount\s*:?\s*rs', r'balance.*rs', r'transaction.*amount',
                r'debited.*rs', r'credited.*rs', r'available balance'
            ]
        }

        self.statement_patterns = {
            'subject_keywords': [
                'statement', 'e-statement', 'estatement', 'credit card statement',
                'account statement', 'monthly statement', 'combined email statement'
            ],
            'sender_keywords': [
                'statement@', 'estatement@', 'emailstatements', 'smartstatement'
            ],
            'content_indicators': [
                'statement period', 'billing period', 'statement date',
                'pdf', 'attachment', 'view statement'
            ]
        }

        self.exclude_patterns = {
            'promotional': [
                'offer', 'discount', 'cashback offer', 'reward point', 'congratulations',
                'welcome offer', 'upgrade offer', 'apply now', 'limited time offer',
                'festive offer', 'bonus point', 'gift voucher', 'win prize', 'lucky draw'
            ],
            'security_alerts': [
                'otp verification', 'password change', 'login alert', 'new device login',
                'pin change', 'security alert', 'account locked', 'suspicious activity'
            ]
        }

    def classify_banking_emails(self, emails: List[Dict]) -> Dict:
        """
        Primary email classification method - replaces old pattern matching
        """
        results = {
            'transaction_emails': [],
            'statement_emails': [],
            'excluded_emails': [],
            'stats': {
                'total': len(emails),
                'processed': 0,
                'excluded': 0,
                'transaction_alerts': 0,
                'statements': 0
            }
        }

        for email in emails:
            should_process, email_type, reason = self._classify_single_email(email)
            
            if should_process:
                email['classification'] = email_type
                email['bank'] = self._get_bank_from_sender(email.get('sender', ''), email_type)
                
                if email_type == 'transaction':
                    results['transaction_emails'].append(email)
                    results['stats']['transaction_alerts'] += 1
                elif email_type == 'statement':
                    results['statement_emails'].append(email)
                    results['stats']['statements'] += 1
                    
                results['stats']['processed'] += 1
            else:
                email['exclusion_reason'] = reason
                results['excluded_emails'].append(email)
                results['stats']['excluded'] += 1

        self.logger.info(f"Email classification complete: {results['stats']}")
        return results

    def _classify_single_email(self, email_data: Dict) -> Tuple[bool, Optional[str], str]:
        """
        Classify individual email and determine if it should be processed.
        Returns: (should_process, email_type, reason)
        """
        subject = email_data.get('subject', '').lower()
        sender = email_data.get('sender', '').lower()
        body = email_data.get('message', '').lower()

        if not self._is_banking_email(sender):
            return False, None, "Not from banking domain"

        if self._is_excluded_email(subject, body):
            return False, None, "Promotional/excluded content"

        if self._is_statement_email(subject, sender, body):
            return True, 'statement', "Statement email detected"

        if self._is_transaction_alert(subject, sender, body):
            return True, 'transaction', "Transaction alert detected"

        return False, None, "No matching banking patterns"

    def _is_banking_email(self, sender: str) -> bool:
        for domain in self.bank_domains.keys():
            if domain in sender:
                return True
        return False

    def _is_excluded_email(self, subject: str, body: str) -> bool:
        content = f"{subject} {body}".lower()
        
        for category, keywords in self.exclude_patterns.items():
            for keyword in keywords:
                if keyword.lower() in content:
                    if category == 'promotional':
                        # Don't exclude if it contains transaction indicators
                        if any(indicator in content for indicator in ['rs.', '₹', 'amount', 'balance', 'transaction']):
                            continue
                    
                    self.logger.debug(f"Email excluded due to {category}: '{keyword}'")
                    return True
        return False

    def _is_statement_email(self, subject: str, sender: str, body: str) -> bool:
        score = 0
        
        for keyword in self.statement_patterns['subject_keywords']:
            if keyword in subject:
                score += 3
                
        for keyword in self.statement_patterns['sender_keywords']:
            if keyword in sender:
                score += 2
                
        content = f"{subject} {body}"
        for pattern in self.statement_patterns['content_indicators']:
            if re.search(pattern, content, re.IGNORECASE):
                score += 1

        self.logger.debug(f"Statement score: {score} for subject: {subject[:50]}")
        return score >= 3

    def _is_transaction_alert(self, subject: str, sender: str, body: str) -> bool:
        score = 0
        
        for keyword in self.transaction_alert_patterns['subject_keywords']:
            if keyword in subject:
                score += 3
                
        for keyword in self.transaction_alert_patterns['sender_keywords']:
            if keyword in sender:
                score += 2

        content = f"{subject} {body}"
        for pattern in self.transaction_alert_patterns['content_indicators']:
            if re.search(pattern, content, re.IGNORECASE):
                score += 2

        self.logger.debug(f"Transaction score: {score} for subject: {subject[:50]}")
        return score >= 4

    def _get_bank_from_sender(self, sender: str, email_type: str = 'transaction') -> str:
        sender_lower = sender.lower()
        
        for domain, bank_enums in self.bank_domains.items():
            if domain in sender_lower:
                # Choose the appropriate enum based on sender and email type
                if 'hdfcbank' in domain:
                    # For HDFC, check the sender to determine if it's credit or debit
                    if 'credit' in sender_lower or 'millennia' in sender_lower or 'cards' in sender_lower:
                        return 'Millenia_Credit'
                    else:
                        return 'HDFC_DEBIT'
                elif 'yesbank' in domain:
                    # For YES Bank, check if it's ACE credit card or regular debit
                    if 'ace' in sender_lower or 'credit' in sender_lower:
                        return 'YES_BANK_ACE'
                    else:
                        return 'YES_BANK_DEBIT'
                else:
                    # For other banks, return the first (and usually only) enum value
                    return bank_enums[0]
                
        return 'UNKNOWN'

    def get_banking_emails_from_all(self, all_emails: List[Dict]) -> List[Dict]:
        """
        Filter all emails to get only banking-related emails
        Primary replacement for pattern-based Gmail queries
        """
        banking_emails = []
        banking_senders = []
        excluded_count = 0
        
        for email in all_emails:
            sender = email.get('sender', '').lower()
            subject = email.get('subject', '')
            
            if self._is_banking_email(sender):
                banking_senders.append(sender)
                should_process, email_type, reason = self._classify_single_email(email)
                if should_process:
                    email['classification'] = email_type
                    bank_enum = self._get_bank_from_sender(sender, email_type)
                    email['bank'] = bank_enum
                    banking_emails.append(email)
                    self.logger.debug(f"✅ Banking email accepted: {subject[:50]} from {sender} -> {bank_enum}")
                else:
                    excluded_count += 1
                    self.logger.debug(f"❌ Banking email excluded: {subject[:50]} from {sender} - {reason}")
            else:
                # Log a few non-banking senders for debugging
                if len(banking_senders) < 10:  # Only log first few to avoid spam
                    self.logger.debug(f"🚫 Non-banking sender: {sender}")

        self.logger.info(f"Found {len(banking_emails)} banking emails out of {len(all_emails)} total emails")
        self.logger.info(f"Banking senders found: {len(banking_senders)}, excluded after classification: {excluded_count}")
        if banking_senders:
            self.logger.debug(f"Banking senders: {list(set(banking_senders))}")
        return banking_emails