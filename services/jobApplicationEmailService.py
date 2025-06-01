"""
Email Processing Service for Job Applications
Rule-based classification with comprehensive logging
"""

import gc
import time
from utils.logger import Logger
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
import re

from models.jobApplicationEmail import JobApplicationEmail
from services.Base_Service import BaseService

class JobApplicationEmailService(BaseService):
    """
    Email processing service for job application detection
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JobApplicationEmailService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()
        self.logger.info("Initializing JobApplicationEmailService")

        self.processing_stats = {
            'processed': 0,
            'job_related': 0,
            'errors': 0
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

        self.logger.info("JobApplicationEmailService initialized successfully")
        self.logger.info(f"Loaded {len(self.job_application_patterns)} pattern categories")
        self.logger.info(f"Loaded {len(self.job_context_terms)} job context terms")
        self.logger.info(f"Loaded {len(self.job_titles)} job titles")

    def _enhanced_rule_classification(self, subject: str, body: str) -> Tuple[bool, float]:
        """
        High-accuracy pattern-based classification for job application emails
        Based on real job application email patterns
        """
        self.logger.debug(f"Starting classification for email with subject: '{subject[:50]}...'")
        full_text = ((subject or "") + " " + (body or "")).lower()
        original_length = len(full_text)

        # Remove extra whitespace and normalize
        full_text = ' '.join(full_text.split())
        self.logger.debug(f"Text normalized: {original_length} -> {len(full_text)} characters")

        confidence_score = 0.0
        reasons = []

        # 1. ACKNOWLEDGMENT PATTERNS (Highest confidence - 40 points)
        self.logger.debug("Checking acknowledgment patterns...")
        acknowledgment_score = 0
        for pattern in self.job_application_patterns['acknowledgment_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                acknowledgment_score = 40
                reasons.append(f"Acknowledgment: {pattern}")
                self.logger.debug(f"✓ Acknowledgment pattern matched: {pattern}")
                break
        confidence_score += acknowledgment_score
        if acknowledgment_score == 0:
            self.logger.debug("✗ No acknowledgment patterns matched")

        # 2. SUBMISSION PATTERNS (High confidence - 30 points)
        self.logger.debug("Checking submission patterns...")
        submission_score = 0
        for pattern in self.job_application_patterns['submission_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                submission_score = 30
                reasons.append(f"Submission: {pattern}")
                self.logger.debug(f"✓ Submission pattern matched: {pattern}")
                break
        confidence_score += submission_score
        if submission_score == 0:
            self.logger.debug("✗ No submission patterns matched")

        # 3. COMPANY + POSITION COMBINATIONS (Very high confidence - 35 points)
        self.logger.debug("Checking company+position patterns...")
        company_position_score = 0
        for pattern in self.job_application_patterns['company_position_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                company_position_score = 35
                reasons.append(f"Company+Position: {pattern}")
                self.logger.debug(f"✓ Company+Position pattern matched: {pattern}")
                break
        confidence_score += company_position_score
        if company_position_score == 0:
            self.logger.debug("✗ No company+position patterns matched")

        # 4. INTERVIEW INVITATIONS (High confidence - 35 points)
        self.logger.debug("Checking interview patterns...")
        interview_score = 0
        for pattern in self.job_application_patterns['interview_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                interview_score = 35
                reasons.append(f"Interview: {pattern}")
                self.logger.debug(f"✓ Interview pattern matched: {pattern}")
                break
        confidence_score += interview_score
        if interview_score == 0:
            self.logger.debug("✗ No interview patterns matched")

        # 5. STATUS UPDATES (Medium confidence - 25 points)
        self.logger.debug("Checking status patterns...")
        status_score = 0
        for pattern in self.job_application_patterns['status_phrases']:
            if re.search(pattern, full_text, re.IGNORECASE):
                status_score = 25
                reasons.append(f"Status: {pattern}")
                self.logger.debug(f"✓ Status pattern matched: {pattern}")
                break
        confidence_score += status_score
        if status_score == 0:
            self.logger.debug("✗ No status patterns matched")

        # 6. JOB CONTEXT TERMS (Supporting evidence - up to 15 points)
        self.logger.debug("Checking job context terms...")
        context_matches = 0
        matched_terms = []
        for term in self.job_context_terms:
            if term in full_text:
                context_matches += 1
                matched_terms.append(term)
        context_score = min(context_matches * 2, 15)  # Max 15 points
        confidence_score += context_score
        if context_score > 0:
            reasons.append(f"Context terms: {context_matches}")
            self.logger.debug(f"✓ Context terms found ({context_matches}): {matched_terms[:5]}...")
        else:
            self.logger.debug("✗ No job context terms found")

        # 7. JOB TITLES (Supporting evidence - up to 10 points)
        self.logger.debug("Checking job titles...")
        title_matches = 0
        matched_titles = []
        for title in self.job_titles:
            if title in full_text:
                title_matches += 1
                matched_titles.append(title)
        title_score = min(title_matches * 3, 10)  # Max 10 points
        confidence_score += title_score
        if title_score > 0:
            reasons.append(f"Job titles: {title_matches}")
            self.logger.debug(f"✓ Job titles found ({title_matches}): {matched_titles[:3]}...")
        else:
            self.logger.debug("✗ No job titles found")

        # 8. SPECIFIC PATTERN MATCHING for your example
        self.logger.debug("Checking specific patterns...")
        specific_patterns = [
            r'hi \w+.+thank you for your interest in \w+',
            r'thank you for your interest.+received your application',
            r'we are delighted that you would consider joining',
            r'consider joining our team',
            r'received your application for \w+',
            r'application for .+, and we are',
        ]

        specific_matches = 0
        for pattern in specific_patterns:
            if re.search(pattern, full_text, re.IGNORECASE):
                confidence_score += 25  # High bonus for specific patterns
                reasons.append(f"Specific pattern: {pattern}")
                self.logger.debug(f"✓ Specific pattern matched: {pattern}")
                specific_matches += 1

        if specific_matches == 0:
            self.logger.debug("✗ No specific patterns matched")

        # 9. NEGATIVE INDICATORS (Reduce confidence)
        self.logger.debug("Checking for spam indicators...")
        spam_patterns = [
            r'click here', r'limited time', r'act now', r'free trial',
            r'unsubscribe', r'marketing', r'promotion', r'sale',
            r'discount', r'offer expires', r'buy now'
        ]

        spam_matches = 0
        for pattern in spam_patterns:
            if re.search(pattern, full_text, re.IGNORECASE):
                confidence_score -= 20
                reasons.append(f"Spam indicator: {pattern}")
                self.logger.debug(f"⚠ Spam indicator found: {pattern}")
                spam_matches += 1

        if spam_matches == 0:
            self.logger.debug("✓ No spam indicators found")

        # 10. COMPANY NAME EXTRACTION BONUS
        self.logger.debug("Checking for company names...")
        company_indicators = re.findall(r'\b[A-Z][a-z]+ (?:Inc|Corp|LLC|Ltd|Technologies|Systems|Solutions|Company)\b',
                                        subject + " " + body)
        if company_indicators and any(term in full_text for term in self.job_context_terms):
            confidence_score += 15
            reasons.append(f"Company name found: {company_indicators[0]}")
            self.logger.debug(f"✓ Company name found: {company_indicators[0]}")
        else:
            self.logger.debug("✗ No company names found")

        # 11. EMAIL STRUCTURE ANALYSIS
        self.logger.debug("Checking email structure...")
        if 'hi ' in full_text[:20].lower() and 'thank you' in full_text:
            confidence_score += 10
            reasons.append("Professional greeting + thank you structure")
            self.logger.debug("✓ Professional greeting structure detected")
        else:
            self.logger.debug("✗ No professional greeting structure")

        # Convert to 0-1 scale (max possible score ~180)
        normalized_confidence = min(confidence_score / 100.0, 1.0)

        # Decision threshold - tuned for high accuracy
        is_job_related = normalized_confidence > 0.3

        # Log classification results
        self.logger.info(
            f"Classification complete: {is_job_related} (confidence: {normalized_confidence:.3f}, raw score: {confidence_score})")
        self.logger.info(f"Matching reasons: {len(reasons)} - {reasons}")
        self.logger.debug(f"Email text preview: {full_text[:150]}...")

        return is_job_related, normalized_confidence

    def process_emails_safely(self, email_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process emails with comprehensive logging
        """
        self.logger.info("=" * 60)
        self.logger.info(f"STARTING EMAIL PROCESSING SESSION")
        self.logger.info(f"Total emails to process: {len(email_list)}")
        self.logger.info("=" * 60)

        start_time = time.time()
        results = {
            'processed': 0,
            'job_emails_found': 0,
            'errors': [],
            'skipped': 0,
            'duplicates': 0
        }

        try:
            # Process emails one by one
            for i, email in enumerate(email_list):
                current_email_num = i + 1
                self.logger.info(f"\n--- Processing Email {current_email_num}/{len(email_list)} ---")

                try:
                    # Log email basic info
                    email_date = email.get('date', 'No date')
                    email_subject = email.get('subject', 'No subject')
                    email_body_length = len(email.get('body', ''))
                    gmail_message_id = email.get('message_id')

                    # Skip if no message ID (shouldn't happen, but just in case)
                    if not gmail_message_id:
                        self.logger.warning(f"Email {current_email_num} has no message ID, skipping")
                        results['skipped'] += 1
                        continue

                    self.logger.info(f"Email Date: {email_date}")
                    self.logger.info(f"Email Subject: '{email_subject}'")
                    self.logger.info(f"Email Body Length: {email_body_length} characters")
                    self.logger.info(f"Gmail Message ID: {gmail_message_id}")

                    # Check for empty emails
                    if not email_subject and not email.get('body'):
                        self.logger.warning(f"Email {current_email_num} is empty, skipping")
                        results['skipped'] += 1
                        continue

                    # Prepare text with safety limits
                    subject = (email.get('subject') or "")[:100]
                    body = (email.get('body') or "")[:200]
                    self.logger.debug(f"Using subject: '{subject}'")
                    self.logger.debug(f"Using body preview: '{body[:50]}...'")

                    # Classify using rule-based approach
                    self.logger.info("Starting email classification...")
                    is_job_related, confidence = self._enhanced_rule_classification(subject, body)

                    if is_job_related:
                        self.logger.info(f"🎯 JOB-RELATED EMAIL DETECTED! Confidence: {confidence:.3f}")

                        # Extract details
                        self.logger.info("Extracting employer and role details...")
                        try:
                            employer, role = self.extract_details(subject, body)
                            self.logger.info(f"Extraction results - Employer: '{employer}', Role: '{role}'")

                            # Check if this email has already been processed
                            existing_email = self.db.session.query(JobApplicationEmail).filter_by(
                                gmail_message_id=gmail_message_id
                            ).first()

                            if existing_email:
                                self.logger.info(f"Email {gmail_message_id} already processed, skipping")
                                results['duplicates'] += 1
                                continue

                            # Create job email object
                            job_email = JobApplicationEmail(
                                email_date=email.get('date', datetime.utcnow()),
                                employer=employer,
                                role=role,
                                email_subject=email.get('subject'),
                                email_body=email.get('body'),
                                gmail_message_id=gmail_message_id
                            )

                            # Save to database
                            self.logger.info("Saving job email to database...")
                            self.db.session.add(job_email)
                            self.db.session.commit()
                            self.logger.info("✅ Job email saved successfully!")

                            results['job_emails_found'] += 1

                        except Exception as e:
                            error_msg = f"Failed to save job email {current_email_num}: {str(e)}"
                            self.logger.error(error_msg, exc_info=True)
                            results['errors'].append(error_msg)
                            self.db.session.rollback()
                            self.logger.error("Database transaction rolled back")
                    else:
                        self.logger.info(f"❌ Email not job-related (confidence: {confidence:.3f})")

                    results['processed'] += 1

                    # Progress reporting every 10 emails
                    if current_email_num % 10 == 0:
                        elapsed_time = time.time() - start_time
                        avg_time_per_email = elapsed_time / current_email_num
                        remaining_emails = len(email_list) - current_email_num
                        estimated_remaining_time = avg_time_per_email * remaining_emails

                        self.logger.info("\n" + "=" * 50)
                        self.logger.info(f"PROGRESS REPORT - Email {current_email_num}/{len(email_list)}")
                        self.logger.info(f"Job emails found: {results['job_emails_found']}")
                        self.logger.info(f"Duplicates skipped: {results['duplicates']}")
                        self.logger.info(f"Errors: {len(results['errors'])}")
                        self.logger.info(f"Elapsed time: {elapsed_time:.1f}s")
                        self.logger.info(f"Avg time per email: {avg_time_per_email:.2f}s")
                        self.logger.info(f"Estimated remaining time: {estimated_remaining_time:.1f}s")
                        self.logger.info("=" * 50 + "\n")

                except Exception as e:
                    error_msg = f"Failed to process email {current_email_num}: {str(e)}"
                    self.logger.error(error_msg, exc_info=True)
                    results['errors'].append(error_msg)
                    continue

        except Exception as e:
            error_msg = f"Critical error in email processing: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            results['errors'].append(error_msg)

        finally:
            # Final cleanup and summary
            gc.collect()
            total_time = time.time() - start_time

            self.logger.info("\n" + "=" * 60)
            self.logger.info("PROCESSING SESSION COMPLETE!")
            self.logger.info("=" * 60)
            self.logger.info(f"Total emails processed: {results['processed']}")
            self.logger.info(f"Job-related emails found: {results['job_emails_found']}")
            self.logger.info(f"Duplicates skipped: {results['duplicates']}")
            self.logger.info(f"Emails skipped: {results['skipped']}")
            self.logger.info(f"Errors encountered: {len(results['errors'])}")
            self.logger.info(f"Total processing time: {total_time:.2f} seconds")
            self.logger.info(f"Average time per email: {total_time / max(1, results['processed']):.2f} seconds")

            if results['errors']:
                self.logger.error("ERRORS SUMMARY:")
                for i, error in enumerate(results['errors'], 1):
                    self.logger.error(f"  {i}. {error}")

            self.logger.info("=" * 60)

        return results

    def extract_details(self, subject: str, body: str) -> Tuple[Optional[str], Optional[str]]:
        """Enhanced detail extraction based on common job email patterns"""
        self.logger.debug("Starting detail extraction process")
        employer, role = None, None

        # Combine and limit text
        full_text = ((subject or "") + "\n" + (body or ""))
        text_preview = full_text[:800]
        self.logger.debug(f"Working with {len(text_preview)} characters of text")

        try:
            # EMPLOYER EXTRACTION
            self.logger.debug("Starting employer extraction...")
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

            for i, pattern in enumerate(employer_patterns):
                if not employer:
                    self.logger.debug(f"Trying employer pattern {i + 1}: {pattern}")
                    matches = re.findall(pattern, text_preview, re.IGNORECASE | re.MULTILINE)
                    if matches:
                        potential_employer = matches[0].strip()
                        self.logger.debug(f"Potential employer found: '{potential_employer}'")

                        # Filter out common false positives
                        if (2 <= len(potential_employer) <= 50 and
                                potential_employer.lower() not in ['we', 'our', 'you', 'your', 'the', 'and', 'team']):
                            employer = potential_employer
                            self.logger.info(f"✅ Employer extracted: '{employer}' using pattern {i + 1}")
                            break
                        else:
                            self.logger.debug(f"Employer candidate rejected: '{potential_employer}'")

            if not employer:
                self.logger.debug("❌ No employer found using any pattern")

            # ROLE EXTRACTION
            self.logger.debug("Starting role extraction...")
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

            for i, pattern in enumerate(role_patterns):
                if not role:
                    self.logger.debug(f"Trying role pattern {i + 1}: {pattern}")
                    matches = re.findall(pattern, text_preview, re.IGNORECASE | re.MULTILINE)
                    if matches:
                        potential_role = matches[0].strip()
                        self.logger.debug(f"Potential role found: '{potential_role}'")

                        # Clean up the role
                        potential_role = re.sub(r'\s+', ' ', potential_role)  # Normalize whitespace
                        potential_role = potential_role.rstrip('.,')  # Remove trailing punctuation

                        # Filter out false positives and validate
                        if (3 <= len(potential_role) <= 60 and
                                potential_role.lower() not in ['your', 'our', 'the', 'and', 'with', 'this', 'that']):
                            role = potential_role
                            self.logger.info(f"✅ Role extracted: '{role}' using pattern {i + 1}")
                            break
                        else:
                            self.logger.debug(f"Role candidate rejected: '{potential_role}'")

            # FALLBACK ROLE DETECTION
            if not role:
                self.logger.debug("Starting fallback role detection...")
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
                        self.logger.info(f"✅ Role found via common job titles: '{job_title}'")
                        break

                # If still no role, look for individual title words
                if not role:
                    self.logger.debug("Trying individual title words...")
                    title_words = ['engineer', 'developer', 'manager', 'analyst', 'designer',
                                   'scientist', 'consultant', 'specialist', 'lead', 'director']
                    for word in title_words:
                        if word in text_lower:
                            # Try to get context around the word
                            word_pattern = rf'\b([A-Za-z]+\s+)*{word}(\s+[A-Za-z]+)*\b'
                            matches = re.findall(word_pattern, text_preview, re.IGNORECASE)
                            if matches:
                                role = word.capitalize()
                                self.logger.info(f"✅ Role found via title word: '{word}' -> '{role}'")
                                break

            if not role:
                self.logger.debug("❌ No role found using any method")

            # CLEAN UP EXTRACTED VALUES
            if employer:
                self.logger.debug(f"Cleaning up employer: '{employer}'")
                # Remove common prefixes/suffixes that might be caught
                employer = re.sub(r'^(the\s+|a\s+)', '', employer, flags=re.IGNORECASE)
                employer = employer.strip()
                if len(employer) > 50:
                    employer = employer[:50].strip()
                self.logger.debug(f"Cleaned employer: '{employer}'")

            if role:
                self.logger.debug(f"Cleaning up role: '{role}'")
                # Clean up role
                role = re.sub(r'^(the\s+|a\s+)', '', role, flags=re.IGNORECASE)
                role = role.strip()
                if len(role) > 60:
                    role = role[:60].strip()
                self.logger.debug(f"Cleaned role: '{role}'")

            self.logger.info(f"Detail extraction complete - Final results: Employer='{employer}', Role='{role}'")

        except Exception as e:
            self.logger.error(f"Error in detail extraction: {e}", exc_info=True)
            # Don't crash, return what we have

        return employer, role

    def get_paginated_job_emails(
            self,
            page: int = 1,
            per_page: int = 10,
            sort_by: str = "created_at",
            sort_order: str = "desc",
            employer_filter: Optional[str] = None,
            role_filter: Optional[str] = None,
            verdict_filter: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get paginated job application emails with optional filtering and sorting.

        Args:
            page: Page number (1-based)
            per_page: Number of items per page
            sort_by: Column to sort by (id, email_date, employer, role, created_at, verdict)
            sort_order: Sort order ('asc' or 'desc')
            employer_filter: Filter by employer name (partial match)
            role_filter: Filter by role name (partial match)
            verdict_filter: Filter by verdict value

        Returns:
            Dictionary containing paginated data and metadata
        """
        self.logger.info(f"Getting paginated job emails - Page: {page}, Per page: {per_page}")

        try:
            # Validate inputs
            if page < 1:
                page = 1
            if per_page < 1:
                per_page = 10
            if per_page > 100:  # Limit max items per page
                per_page = 100

            # Valid sort columns
            valid_sort_columns = ['id', 'email_date', 'employer', 'role', 'created_at', 'verdict']
            if sort_by not in valid_sort_columns:
                sort_by = 'created_at'

            # Build base query
            query = self.db.session.query(JobApplicationEmail)

            # Apply filters
            if employer_filter:
                query = query.filter(JobApplicationEmail.employer.ilike(f'%{employer_filter}%'))
                self.logger.debug(f"Applied employer filter: {employer_filter}")

            if role_filter:
                query = query.filter(JobApplicationEmail.role.ilike(f'%{role_filter}%'))
                self.logger.debug(f"Applied role filter: {role_filter}")

            if verdict_filter is not None:
                query = query.filter(JobApplicationEmail.verdict == verdict_filter)
                self.logger.debug(f"Applied verdict filter: {verdict_filter}")

            # Get total count before pagination
            total = query.count()
            self.logger.debug(f"Total records after filtering: {total}")

            # Apply sorting
            sort_column = getattr(JobApplicationEmail, sort_by)
            if sort_order.lower() == 'asc':
                query = query.order_by(sort_column.asc())
            else:
                query = query.order_by(sort_column.desc())

            # Apply pagination
            offset = (page - 1) * per_page
            items = query.offset(offset).limit(per_page).all()

            # Calculate pagination metadata
            total_pages = (total + per_page - 1) // per_page  # Ceiling division
            has_next = page < total_pages
            has_prev = page > 1
            next_page = page + 1 if has_next else None
            prev_page = page - 1 if has_prev else None

            # Convert items to dictionaries for JSON serialization
            items_data = []
            for item in items:
                items_data.append({
                    'id': item.id,
                    'email_date': item.email_date.isoformat() if item.email_date else None,
                    'employer': item.employer,
                    'role': item.role,
                    'email_subject': item.email_subject,
                    'email_body': item.email_body,
                    'created_at': item.created_at.isoformat() if item.created_at else None,
                    'verdict': item.verdict
                })

            result = {
                'items': items_data,
                'pagination': {
                    'total': total,
                    'page': page,
                    'per_page': per_page,
                    'total_pages': total_pages,
                    'has_next': has_next,
                    'has_prev': has_prev,
                    'next_page': next_page,
                    'prev_page': prev_page
                }
            }

            self.logger.info(f"Successfully retrieved {len(items_data)} items (page {page}/{total_pages})")
            return result

        except Exception as e:
            self.logger.error(f"Error in get_paginated_job_emails: {e}", exc_info=True)
            return {
                'items': [],
                'pagination': {
                    'total': 0,
                    'page': 1,
                    'per_page': per_page,
                    'total_pages': 0,
                    'has_next': False,
                    'has_prev': False,
                    'next_page': None,
                    'prev_page': None
                },
                'error': str(e)
            }

    def update_job_application(self, job_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a job application email record with the provided updates.
        
        Args:
            job_id: ID of the job application to update
            updates: Dictionary of column names and their new values
            
        Returns:
            Dictionary with status and updated record
        """
        try:
            # Get the job application
            job_app = self.db.session.query(JobApplicationEmail).filter_by(id=job_id).first()
            
            if not job_app:
                return {
                    'status': 'error',
                    'message': f'Job application with ID {job_id} not found'
                }
            
            # Validate the columns to update
            valid_columns = {'employer', 'role', 'email_subject', 'email_body', 'verdict', 'email_date'}
            invalid_columns = set(updates.keys()) - valid_columns
            
            if invalid_columns:
                return {
                    'status': 'error',
                    'message': f'Invalid columns: {", ".join(invalid_columns)}'
                }
            
            # Apply updates
            for column, value in updates.items():
                if column == 'email_date' and value:
                    # Convert string date to datetime if needed
                    if isinstance(value, str):
                        try:
                            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        except ValueError:
                            return {
                                'status': 'error',
                                'message': f'Invalid date format for email_date: {value}'
                            }
                setattr(job_app, column, value)
            
            self.db.session.commit()
            
            # Return updated record
            return {
                'status': 'success',
                'data': {
                    'id': job_app.id,
                    'email_date': job_app.email_date.isoformat() if job_app.email_date else None,
                    'employer': job_app.employer,
                    'role': job_app.role,
                    'email_subject': job_app.email_subject,
                    'email_body': job_app.email_body,
                    'created_at': job_app.created_at.isoformat() if job_app.created_at else None,
                    'verdict': job_app.verdict
                }
            }
            
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating job application: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to update job application: {str(e)}'
            }