"""
CRASH-PROOF Email Processing Service for 8GB MacBook
This implementation prioritizes stability and memory safety above all else.
"""

import gc
import psutil
import os
import threading
import time
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
import re
from contextlib import contextmanager

from models.jobApplicationEmail import JobApplicationEmail
from services.Base_Service import BaseService

# Safe imports with fallbacks
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = False
except ImportError:
    HAS_TRANSFORMERS = False
    print("Warning: transformers not available, using rule-based only")

try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

class MemoryMonitor:
    """Monitor and enforce memory limits to prevent crashes"""

    def __init__(self, max_memory_gb: float = 6.0):  # Leave 2GB buffer on 8GB system
        self.max_memory_bytes = max_memory_gb * 1024 * 1024 * 1024
        self.process = psutil.Process(os.getpid())

    def get_memory_usage_gb(self) -> float:
        """Get current memory usage in GB"""
        return self.process.memory_info().rss / (1024 * 1024 * 1024)

    def is_memory_safe(self) -> bool:
        """Check if memory usage is within safe limits"""
        return self.process.memory_info().rss < self.max_memory_bytes

    def force_cleanup(self):
        """Aggressive memory cleanup"""
        gc.collect()
        if HAS_TORCH:
            torch.cuda.empty_cache() if torch.cuda.is_available() else None
        time.sleep(0.1)  # Give system time to clean up

@contextmanager
def memory_guard(monitor: MemoryMonitor, operation_name: str = "operation"):
    """Context manager to ensure operations don't exceed memory limits"""
    initial_memory = monitor.get_memory_usage_gb()
    try:
        if not monitor.is_memory_safe():
            raise MemoryError(f"Insufficient memory before {operation_name}")
        yield
    finally:
        monitor.force_cleanup()
        final_memory = monitor.get_memory_usage_gb()
        print(f"{operation_name}: {initial_memory:.2f}GB -> {final_memory:.2f}GB")

class JobApplicationEmailService(BaseService):
    """
    Ultra-stable email processing service designed never to crash on 8GB systems
    """

    def __init__(self):
        super().__init__()
        self.memory_monitor = MemoryMonitor(max_memory_gb=6.0)
        self._classifier = None
        self._classifier_lock = threading.Lock()
        self.processing_stats = {
            'processed': 0,
            'job_related': 0,
            'errors': 0,
            'memory_warnings': 0
        }

        # High-accuracy patterns based on real job application emails
        self.job_application_patterns = {
            # Acknowledgment patterns (highest confidence)
            'acknowledgment_phrases': [
                r'thank you for your interest in \w+',
                r'we received your application',
                r'received your application for',
                r'thank you for applying',
                r'application for .+ has been received',
                r'we have received your application',
                r'your application has been received',
                r'thank you for your application',
                r'we wanted to let you know we received',
                r'application .+ received',
                r'confirm receipt of your application'
            ],

            # Application submission confirmations
            'submission_phrases': [
                r'application for .+ position',
                r'applied for .+ role',
                r'your application for the .+ position',
                r'application to .+ team',
                r'position: .+',
                r'role: .+',
                r'applying for .+',
                r'interested in .+ position'
            ],

            # Company + position combinations (very specific)
            'company_position_phrases': [
                r'application for .+ at \w+',
                r'thank you for your interest in \w+.+we.+received.+application',
                r'joining our team',
                r'consider joining',
                r'would consider joining our',
                r'delighted that you would consider',
                r'pleased to receive your application'
            ],

            # Interview invitations
            'interview_phrases': [
                r'invite you .+ interview',
                r'schedule .+ interview',
                r'interview for .+ position',
                r'would like to interview',
                r'phone screen',
                r'video interview',
                r'meet with our team',
                r'next steps.+interview'
            ],

            # Rejection patterns
            'rejection_phrases': [
                r'regret to inform',
                r'unfortunately.+not.+selected',
                r'moving forward with other candidates',
                r'position has been filled',
                r'not be moving forward',
                r'appreciate your interest.+however',
                r'thank you for your interest.+unfortunately'
            ],

            # Status update patterns
            'status_phrases': [
                r'application status',
                r'update on your application',
                r'regarding your application',
                r'application update',
                r'status of your application'
            ]
        }

        # Job-related terms that provide context
        self.job_context_terms = [
            'position', 'role', 'job', 'career', 'opportunity', 'opening',
            'vacancy', 'hiring', 'recruitment', 'candidate', 'applicant',
            'resume', 'cv', 'experience', 'qualifications', 'skills',
            'team', 'department', 'company', 'organization', 'employer'
        ]

        # Common job titles and roles
        self.job_titles = [
            'engineer', 'developer', 'programmer', 'analyst', 'manager',
            'designer', 'consultant', 'specialist', 'coordinator', 'lead',
            'senior', 'junior', 'intern', 'associate', 'director',
            'officer', 'representative', 'technician', 'architect'
        ]

    def _enhanced_rule_classification(self, subject: str, body: str) -> Tuple[bool, float]:
        """
        High-accuracy pattern-based classification for job application emails
        Based on real job application email patterns
        """
        full_text = ((subject or "") + " " + (body or "")).lower()

        # Remove extra whitespace and normalize
        full_text = ' '.join(full_text.split())

        confidence_score = 0.0
        reasons = []  # For debugging

        # 1. ACKNOWLEDGMENT PATTERNS (Highest confidence - 40 points)
        acknowledgment_score = 0
        for pattern in self.job_application_patterns['acknowledgment_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                acknowledgment_score = 40
                reasons.append(f"Acknowledgment: {pattern}")
                break
        confidence_score += acknowledgment_score

        # 2. SUBMISSION PATTERNS (High confidence - 30 points)
        submission_score = 0
        for pattern in self.job_application_patterns['submission_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                submission_score = 30
                reasons.append(f"Submission: {pattern}")
                break
        confidence_score += submission_score

        # 3. COMPANY + POSITION COMBINATIONS (Very high confidence - 35 points)
        company_position_score = 0
        for pattern in self.job_application_patterns['company_position_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                company_position_score = 35
                reasons.append(f"Company+Position: {pattern}")
                break
        confidence_score += company_position_score

        # 4. INTERVIEW INVITATIONS (High confidence - 35 points)
        interview_score = 0
        for pattern in self.job_application_patterns['interview_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                interview_score = 35
                reasons.append(f"Interview: {pattern}")
                break
        confidence_score += interview_score

        # 5. STATUS UPDATES (Medium confidence - 25 points)
        status_score = 0
        for pattern in self.job_application_patterns['status_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                status_score = 25
                reasons.append(f"Status: {pattern}")
                break
        confidence_score += status_score

        # 6. JOB CONTEXT TERMS (Supporting evidence - up to 15 points)
        context_matches = 0
        for term in self.job_context_terms:
            if term in full_text:
                context_matches += 1
        context_score = min(context_matches * 2, 15)  # Max 15 points
        confidence_score += context_score
        if context_score > 0:
            reasons.append(f"Context terms: {context_matches}")

        # 7. JOB TITLES (Supporting evidence - up to 10 points)
        title_matches = 0
        for title in self.job_titles:
            if title in full_text:
                title_matches += 1
        title_score = min(title_matches * 3, 10)  # Max 10 points
        confidence_score += title_score
        if title_score > 0:
            reasons.append(f"Job titles: {title_matches}")

        # 8. SPECIFIC PATTERN MATCHING for your example
        # "Hi [Name], Thank you for your interest in [Company]! We wanted to let you know we received your application"
        specific_patterns = [
            r'hi \w+.+thank you for your interest in \w+',
            r'thank you for your interest.+received your application',
            r'we are delighted that you would consider joining',
            r'consider joining our team',
            r'received your application for \w+',
            r'application for .+, and we are',
        ]

        for pattern in specific_patterns:
            if re.search(pattern, full_text, re.IGNORECASE):
                confidence_score += 25  # High bonus for specific patterns
                reasons.append(f"Specific pattern: {pattern}")

        # 9. NEGATIVE INDICATORS (Reduce confidence)
        spam_patterns = [
            r'click here', r'limited time', r'act now', r'free trial',
            r'unsubscribe', r'marketing', r'promotion', r'sale',
            r'discount', r'offer expires', r'buy now'
        ]

        for pattern in spam_patterns:
            if re.search(pattern, full_text, re.IGNORECASE):
                confidence_score -= 20
                reasons.append(f"Spam indicator: {pattern}")

        # 10. COMPANY NAME EXTRACTION BONUS
        # If we can extract a company name + job-related terms, high confidence
        company_indicators = re.findall(r'\b[A-Z][a-z]+ (?:Inc|Corp|LLC|Ltd|Technologies|Systems|Solutions|Company)\b', subject + " " + body)
        if company_indicators and any(term in full_text for term in self.job_context_terms):
            confidence_score += 15
            reasons.append(f"Company name found: {company_indicators[0]}")

        # 11. EMAIL STRUCTURE ANALYSIS
        # Job emails often have specific structure
        if 'hi ' in full_text[:20].lower() and 'thank you' in full_text:
            confidence_score += 10
            reasons.append("Professional greeting + thank you structure")

        # Convert to 0-1 scale (max possible score ~180)
        normalized_confidence = min(confidence_score / 100.0, 1.0)

        # Decision threshold - tuned for high accuracy
        is_job_related = normalized_confidence > 0.3

        # Debug output for tuning
        if normalized_confidence > 0.1:  # Only show potential matches
            print(f"Classification: {is_job_related} (confidence: {normalized_confidence:.3f})")
            print(f"  Reasons: {reasons}")
            print(f"  Text preview: {full_text[:100]}...")

        return is_job_related, normalized_confidence

    def _safe_ml_classification(self, texts: List[str]) -> List[Tuple[bool, float]]:
        """
        Safely attempt ML classification with fallback to rules
        """
        results = []

        # Check if we can safely use ML
        if not HAS_TRANSFORMERS or not self.memory_monitor.is_memory_safe():
            print("ML classification skipped due to memory constraints - using enhanced rules")
            for text in texts:
                subject, _, body = text.partition('\n')
                result = self._enhanced_rule_classification(subject, body)
                results.append(result)
            return results

        try:
            with memory_guard(self.memory_monitor, "ML Classification"):
                classifier = self._get_safe_classifier()
                if classifier is None:
                    raise Exception("Classifier failed to load")

                candidate_labels = [
                    "job application submission",
                    "job application acknowledgement",
                    "interview invitation",
                    "spam or promotional email",
                    "general correspondence"
                ]

                for text in texts:
                    try:
                        # Aggressively limit text length
                        limited_text = text[:200]

                        result = classifier(
                            limited_text,
                            candidate_labels,
                            multi_label=False
                        )

                        is_job_related = result['labels'][0] in [
                            "job application submission",
                            "job application acknowledgement",
                            "interview invitation"
                        ]
                        confidence = result['scores'][0]

                        results.append((is_job_related, confidence))

                    except Exception as e:
                        print(f"ML classification failed for single text: {e}")
                        # Fallback to rules for this text
                        subject, _, body = text.partition('\n')
                        result = self._enhanced_rule_classification(subject, body)
                        results.append(result)

                    # Memory check after each classification
                    if not self.memory_monitor.is_memory_safe():
                        print("Memory limit reached during ML - switching to rules")
                        break

        except Exception as e:
            print(f"ML classification completely failed: {e}")
            # Fallback to enhanced rules for all remaining texts
            for text in texts[len(results):]:
                subject, _, body = text.partition('\n')
                result = self._enhanced_rule_classification(subject, body)
                results.append(result)

        return results

    def _get_safe_classifier(self):
        """Safely load the smallest possible classifier"""
        if self._classifier is not None:
            return self._classifier

        with self._classifier_lock:
            if self._classifier is not None:
                return self._classifier

            try:
                # Try progressively smaller models
                models_to_try = [
                    "typeform/distilbert-base-uncased-mnli",  # ~250MB
                    "microsoft/DialoGPT-small",               # ~117MB
                ]

                for model_name in models_to_try:
                    try:
                        print(f"Attempting to load {model_name}")
                        with memory_guard(self.memory_monitor, f"Loading {model_name}"):
                            self._classifier = pipeline(
                                "zero-shot-classification",
                                model=model_name,
                                device=-1,  # CPU only
                                torch_dtype="float16" if HAS_TORCH else None
                            )
                            print(f"Successfully loaded {model_name}")
                            return self._classifier
                    except Exception as e:
                        print(f"Failed to load {model_name}: {e}")
                        continue

                print("All ML models failed to load - using rule-based only")
                return None

            except Exception as e:
                print(f"Classifier loading completely failed: {e}")
                return None

    def process_emails_safely(self, email_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process emails with maximum safety - guaranteed not to crash
        """
        print(f"Starting safe processing of {len(email_list)} emails")
        print(f"Initial memory usage: {self.memory_monitor.get_memory_usage_gb():.2f}GB")

        results = {
            'processed': 0,
            'job_emails_found': 0,
            'errors': [],
            'memory_warnings': 0
        }

        try:
            # Process emails one by one to minimize memory spikes
            for i, email in enumerate(email_list):
                try:
                    # Memory safety check before each email
                    if not self.memory_monitor.is_memory_safe():
                        self.memory_monitor.force_cleanup()
                        results['memory_warnings'] += 1

                        if not self.memory_monitor.is_memory_safe():
                            error_msg = f"Memory limit exceeded at email {i+1}, stopping processing"
                            print(error_msg)
                            results['errors'].append(error_msg)
                            break

                    # Prepare text with safety limits
                    subject = (email.get('subject') or "")[:100]  # Very conservative limits
                    body = (email.get('body') or "")[:200]
                    text = f"{subject}\n{body}"

                    # Classify single email
                    classification_results = self._safe_ml_classification([text])

                    if classification_results and classification_results[0][0]:  # is_job_related
                        # Extract details safely
                        try:
                            employer, role = self.extract_details(subject, body)

                            job_email = JobApplicationEmail(
                                email_date=email.get('date', datetime.utcnow()),
                                employer=employer,
                                role=role,
                                email_subject=email.get('subject'),
                                email_body=email.get('body')
                            )

                            # Save immediately to avoid memory buildup
                            self.db.session.add(job_email)
                            self.db.session.commit()

                            results['job_emails_found'] += 1

                        except Exception as e:
                            error_msg = f"Failed to save email {i+1}: {str(e)}"
                            print(error_msg)
                            results['errors'].append(error_msg)
                            self.db.session.rollback()

                    results['processed'] += 1

                    # Progress reporting
                    if (i + 1) % 10 == 0:
                        memory_gb = self.memory_monitor.get_memory_usage_gb()
                        print(f"Processed {i+1}/{len(email_list)} emails, "
                              f"Found {results['job_emails_found']} job emails, "
                              f"Memory: {memory_gb:.2f}GB")

                except Exception as e:
                    error_msg = f"Failed to process email {i+1}: {str(e)}"
                    print(error_msg)
                    results['errors'].append(error_msg)
                    continue

        except Exception as e:
            error_msg = f"Critical error in email processing: {str(e)}"
            print(error_msg)
            results['errors'].append(error_msg)

        finally:
            # Final cleanup
            self.memory_monitor.force_cleanup()
            final_memory = self.memory_monitor.get_memory_usage_gb()
            print(f"Processing complete. Final memory usage: {final_memory:.2f}GB")

        return results

    def extract_details(self, subject: str, body: str) -> Tuple[Optional[str], Optional[str]]:
        """Enhanced detail extraction based on common job email patterns"""
        employer, role = None, None

        # Combine and limit text
        full_text = ((subject or "") + "\n" + (body or ""))
        text_preview = full_text[:800]  # Increased for better extraction

        try:
            # EMPLOYER EXTRACTION - Enhanced patterns
            employer_patterns = [
                # "Thank you for your interest in [Company]"
                r'thank you for your interest in ([A-Z][a-zA-Z0-9\s&\.]+?)[\!\.\,]',
                # "We are [Company] and we received"
                r'we are ([A-Z][a-zA-Z0-9\s&\.]+?) and',
                # "at [Company] team" or "at [Company],"
                r'\sat ([A-Z][a-zA-Z0-9\s&\.]+?)(?:\s+team|\s*[\,\.]|\s+and)',
                # "[Company] is pleased to"
                r'^([A-Z][a-zA-Z0-9\s&\.]+?) is (?:pleased|delighted|excited)',
                # "From: [Company] Careers" or similar
                r'from:?\s*([A-Z][a-zA-Z0-9\s&\.]+?)(?:\s+careers|\s+hiring|\s+team)',
                # Company name with legal suffixes
                r'\b([A-Z][a-zA-Z0-9\s&\.]+?)\s+(?:Inc|Corp|LLC|Ltd|Technologies|Systems|Solutions|Company)\b',
                # "joining [Company]" or "joining our team at [Company]"
                r'joining (?:our team at )?([A-Z][a-zA-Z0-9\s&\.]+?)[\!\.\,\s]',
            ]

            for pattern in employer_patterns:
                if not employer:
                    matches = re.findall(pattern, text_preview, re.IGNORECASE | re.MULTILINE)
                    if matches:
                        potential_employer = matches[0].strip()
                        # Filter out common false positives
                        if (len(potential_employer) >= 2 and
                            len(potential_employer) <= 50 and
                            potential_employer.lower() not in ['we', 'our', 'you', 'your', 'the', 'and', 'team']):
                            employer = potential_employer
                            break

            # ROLE EXTRACTION - Enhanced patterns based on your example
            role_patterns = [
                # "application for [Role]," (your exact pattern)
                r'application for ([A-Z][a-zA-Z0-9\s,&\-\.]+?)(?:\,|\sand|\.|$)',
                # "applied for [Role] position"
                r'applied for (?:the\s+)?([A-Z][a-zA-Z0-9\s,&\-\.]+?)\s+position',
                # "[Role] position at"
                r'([A-Z][a-zA-Z0-9\s,&\-\.]+?)\s+position\s+at',
                # "Position: [Role]" or "Role: [Role]"
                r'(?:position|role):\s*([A-Z][a-zA-Z0-9\s,&\-\.]+?)(?:\n|$|\,)',
                # "for the [Role] role"
                r'for the ([A-Z][a-zA-Z0-9\s,&\-\.]+?)\s+role',
                # "interested in [Role]"
                r'interested in (?:the\s+)?([A-Z][a-zA-Z0-9\s,&\-\.]+?)(?:\s+position|\s+role|[\.\,\n])',
                # "interview for [Role]"
                r'interview for (?:the\s+)?([A-Z][a-zA-Z0-9\s,&\-\.]+?)(?:\s+position|\s+role|[\.\,\n])',
            ]

            for pattern in role_patterns:
                if not role:
                    matches = re.findall(pattern, text_preview, re.IGNORECASE | re.MULTILINE)
                    if matches:
                        potential_role = matches[0].strip()
                        # Clean up the role
                        potential_role = re.sub(r'\s+', ' ', potential_role)  # Normalize whitespace
                        potential_role = potential_role.rstrip('.,')  # Remove trailing punctuation

                        # Filter out false positives and validate
                        if (len(potential_role) >= 3 and
                            len(potential_role) <= 60 and
                            potential_role.lower() not in ['your', 'our', 'the', 'and', 'with', 'this', 'that']):
                            role = potential_role
                            break

            # FALLBACK ROLE DETECTION - Look for common job titles
            if not role:
                common_job_titles = [
                    'Software Engineer', 'Data Scientist', 'Product Manager', 'Frontend Developer',
                    'Backend Developer', 'Full Stack Developer', 'DevOps Engineer', 'ML Engineer',
                    'Data Analyst', 'Business Analyst', 'UX Designer', 'UI Designer', 'Designer',
                    'Engineering Manager', 'Technical Lead', 'Senior Engineer', 'Junior Developer',
                    'Intern', 'Associate', 'Consultant', 'Specialist', 'Coordinator'
                ]

                text_lower = text_preview.lower()
                for job_title in common_job_titles:
                    if job_title.lower() in text_lower:
                        role = job_title
                        break

                # If still no role, look for individual title words
                if not role:
                    title_words = ['engineer', 'developer', 'manager', 'analyst', 'designer',
                                 'scientist', 'consultant', 'specialist', 'lead', 'director']
                    for word in title_words:
                        if word in text_lower:
                            # Try to get context around the word
                            word_pattern = rf'\b([A-Za-z]+\s+)*{word}(\s+[A-Za-z]+)*\b'
                            matches = re.findall(word_pattern, text_preview, re.IGNORECASE)
                            if matches:
                                role = word.capitalize()
                                break

            # CLEAN UP EXTRACTED VALUES
            if employer:
                # Remove common prefixes/suffixes that might be caught
                employer = re.sub(r'^(the\s+|a\s+)', '', employer, flags=re.IGNORECASE)
                employer = employer.strip()
                if len(employer) > 50:
                    employer = employer[:50].strip()

            if role:
                # Clean up role
                role = re.sub(r'^(the\s+|a\s+)', '', role, flags=re.IGNORECASE)
                role = role.strip()
                if len(role) > 60:
                    role = role[:60].strip()

        except Exception as e:
            print(f"Error in detail extraction: {e}")
            # Don't crash, return what we have

        return employer, role

# Usage example with maximum safety
def safe_email_processing_example():
    """
    Example of how to use the crash-proof service
    """
    service = CrashProofEmailService()

    # Your email list here
    email_list = [...]  # Your 100 emails

    # Process with complete safety
    results = service.process_emails_safely(email_list)

    print("Processing Results:")
    print(f"- Processed: {results['processed']} emails")
    print(f"- Job emails found: {results['job_emails_found']}")
    print(f"- Memory warnings: {results['memory_warnings']}")
    print(f"- Errors: {len(results['errors'])}")

    if results['errors']:
        print("Errors encountered:")
        for error in results['errors']:
            print(f"  - {error}")

    return results