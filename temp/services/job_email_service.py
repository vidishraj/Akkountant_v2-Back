import os
import json
import sys
import anyio
from datetime import datetime, timedelta
from services.Base_Service import BaseService
from temp.models.job_emails import JobEmail
from temp.models.processed_emails import ProcessedEmail
from models.googleTokens import UserToken
from enums.ServiceTypeEnum import ServiceTypeEnum
from utils.logger import Logger
from sqlalchemy import and_
from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, TextBlock


class JobEmailService(BaseService):
    
    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()
    
    def _get_gmail_token_for_user(self, user_id: str):
        """Fetch Gmail token for user from database"""
        try:
            user_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type=ServiceTypeEnum.Gmail.value
            ).first()
            
            if not user_token:
                raise ValueError(f"No Gmail token found for user {user_id}")
            
            return {
                'token': user_token.access_token,
                'refresh_token': user_token.refresh_token,
                'client_id': user_token.client_id,
                'client_secret': user_token.client_secret,
            }
        except Exception as e:
            self.logger.error(f"Error fetching Gmail token for user {user_id}: {str(e)}")
            raise

    def scan_emails_for_jobs(self, user_id: str, date_from: str = None, date_to: str = None):
        """Scan emails for job applications and updates using Claude batch processing"""
        try:
            # Get Gmail token for user from database
            token_data = self._get_gmail_token_for_user(user_id)
            
            # Handle date range - if not provided, default to last 7 days
            if date_from and date_to:
                # Convert from YYYY-MM-DD to YYYY/MM/DD format for Gmail API
                date_from_formatted = datetime.strptime(date_from, '%Y-%m-%d').strftime('%Y/%m/%d')
                date_to_formatted = datetime.strptime(date_to, '%Y-%m-%d').strftime('%Y/%m/%d')
            else:
                # Default to last 7 days
                date_to_formatted = datetime.now().strftime('%Y/%m/%d')
                date_from_formatted = (datetime.now() - timedelta(days=7)).strftime('%Y/%m/%d')
            
            self.logger.info(f"Scanning emails from {date_from_formatted} to {date_to_formatted} for user {user_id}")
            
            # Collect all emails first and filter out already processed ones
            all_emails = []
            for email in self.gmailService.findAllEmailsInInterval(user_id, token_data, date_from_formatted, date_to_formatted):
                all_emails.append(email)
            
            # Filter out already processed emails in batch
            unprocessed_emails = self._filter_unprocessed_emails(all_emails, user_id)
            
            self.logger.info(f"Found {len(all_emails)} total emails, {len(unprocessed_emails)} unprocessed emails to analyze")
            
            processed_count = 0
            new_count = 0
            
            # Process emails in batches of 10
            batch_size = 10
            for i in range(0, len(unprocessed_emails), batch_size):
                batch = unprocessed_emails[i:i + batch_size]
                
                try:
                    # Analyze batch with Claude
                    job_emails_data = self._analyze_email_batch_with_claude(batch)
                    
                    # Only mark as processed if Claude analysis succeeded
                    self._mark_emails_as_processed(batch, user_id)
                    
                    # Save only job-related emails
                    for email_data in job_emails_data:
                        if email_data['is_job_related']:
                            self._save_email_backup(email_data['email'])
                            self._save_job_email(email_data['email'], email_data['job_data'], user_id)
                            new_count += 1
                        processed_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing batch {i//batch_size + 1}: {str(e)}")
                    # Do NOT mark as processed - let them be retried next scan
                    self.logger.info(f"Batch {i//batch_size + 1} will be retried on next scan")
                    continue
            
            self.logger.info(f"Processed {processed_count} emails, {new_count} job-related emails saved")
            return {"processed": processed_count, "new": new_count}
            
        except Exception as e:
            self.logger.error(f"Error scanning emails: {str(e)}")
            raise
    
    def _analyze_email_batch_with_claude(self, emails):
        """Analyze batch of emails with Claude to identify job-related ones"""
        try:
            # Prepare batch data for Claude
            email_summaries = []
            for i, email in enumerate(emails):
                email_summaries.append({
                    "id": i,
                    "subject": email.get('subject', ''),
                    "sender": email.get('sender', ''),
                    "snippet": email.get('message', '')[:300]  # Limit to reduce tokens
                })
            
            prompt = self._create_batch_analysis_prompt(email_summaries)
            
            # Call Claude API
            claude_response = self._call_claude_api(prompt)
            
            # Parse Claude's response
            return self._parse_claude_batch_response(claude_response, emails)
            
        except Exception as e:
            self.logger.error(f"Error in Claude batch analysis: {str(e)}")
            # Fallback: raise exception.
            raise Exception
    
    def _filter_unprocessed_emails(self, emails, user_id):
        """Filter out emails that have already been processed"""
        try:
            # Get all gmail_ids from the batch
            gmail_ids = [email['gmail_id'] for email in emails]
            
            # Query processed emails in batch
            processed_gmail_ids = self.db.session.query(ProcessedEmail.gmail_id).filter(
                and_(
                    ProcessedEmail.user_id == user_id,
                    ProcessedEmail.gmail_id.in_(gmail_ids)
                )
            ).all()
            
            # Convert to set for faster lookup
            processed_set = {row[0] for row in processed_gmail_ids}
            
            # Filter out processed emails
            unprocessed = [email for email in emails if email['gmail_id'] not in processed_set]
            
            self.logger.debug(f"Filtered out {len(emails) - len(unprocessed)} already processed emails")
            return unprocessed
            
        except Exception as e:
            self.logger.error(f"Error filtering processed emails: {str(e)}")
            # Return all emails if filtering fails
            return emails
    
    def _mark_emails_as_processed(self, emails, user_id):
        """Mark emails as processed in batch"""
        try:
            processed_emails = []
            for email in emails:
                processed_emails.append(ProcessedEmail(
                    gmail_id=email['gmail_id'],
                    user_id=user_id
                ))
            
            self.db.session.add_all(processed_emails)
            self.db.session.commit()
            
            self.logger.debug(f"Marked {len(emails)} emails as processed")
            
        except Exception as e:
            self.logger.error(f"Error marking emails as processed: {str(e)}")
            self.db.session.rollback()
            # Don't raise - we don't want to fail the whole batch for this
    
    def _create_batch_analysis_prompt(self, email_summaries):
        """Create highly precise prompt for job application analysis"""
        emails_text = ""
        for email in email_summaries:
            emails_text += f"ID:{email['id']}\nFrom:{email['sender']}\nSubject:{email['subject']}\nContent:{email['snippet']}\n---\n"
        
        return f"""CRITICAL: Identify ONLY emails about MY job applications - where I have personally applied to a specific company/position.

INCLUDE ONLY:
- Application confirmations ("Thank you for applying")
- Interview invitations/scheduling
- Application status updates (rejected/accepted/under review)
- Offer letters or decisions
- Follow-up requests for documents/references
- Assessment or test invitations

EXCLUDE ALL:
- Job opening advertisements/listings
- Newsletter about job opportunities
- "Apply now" or recruitment marketing emails
- LinkedIn/job board notifications
- General hiring announcements
- Company newsletters mentioning jobs
- Emails promoting job search services
- Mass recruitment emails not specific to my application
- Upwork related emails

EMAILS TO ANALYZE:
{emails_text}

Return ONLY a JSON array:
[{{"id":0,"is_job_related":true,"company":"CompanyName","job_title":"Position","status":"applied","type":"new_application"}}]

For non-job emails or job ads/marketing: {{"id":X,"is_job_related":false}}
For MY job applications: extract company, job_title, status (applied/interview/offer/rejected/under_review), type (new_application/status_update)"""

    def _call_claude_api(self, prompt):
        """Call Claude API using claude_agent_sdk"""
        try:
            # Configure Claude options for batch job email analysis
            options = ClaudeAgentOptions(
                max_turns=1,
                system_prompt="""You are a precise email classifier that ONLY identifies emails about job applications I have personally submitted.

STRICT CRITERIA - Mark as job-related ONLY if:
1. Email confirms I submitted an application 
2. Email is about MY specific application status/progress
3. Email requests additional info for MY application
4. Email schedules/invites me for interview/assessment

NEVER mark as job-related:
- General job postings or "now hiring" ads
- Marketing emails with job opportunities 
- Newsletters about careers
- Mass recruitment campaigns
- Job board/LinkedIn notifications
- Upwork related emails

Be extremely conservative. When in doubt, mark as false.
Return ONLY valid JSON without explanations."""
            )
            
            # Collect response text using async pattern
            output_text = ""
            
            async def get_claude_response():
                nonlocal output_text
                async for message in query(prompt=prompt, options=options):
                    if isinstance(message, AssistantMessage):
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                output_text += block.text
                                self.logger.debug(f"Received Claude text block: {len(block.text)} characters")
            
            # Run async query in sync context
            anyio.run(get_claude_response)
            
            if not output_text:
                raise Exception("Claude returned empty response")
            
            self.logger.debug(f"Claude response length: {len(output_text)}")
            return output_text
            
        except Exception as e:
            self.logger.error(f"Error calling Claude API: {str(e)}")
            raise

    def _parse_claude_batch_response(self, claude_response, emails):
        """Parse Claude's JSON response and map back to emails"""
        try:
            # Extract JSON from Claude's response
            import re
            json_match = re.search(r'\[.*\]', claude_response, re.DOTALL)
            if not json_match:
                raise ValueError("No JSON array found in Claude response")
            
            analysis_results = json.loads(json_match.group())
            
            # Map results back to emails
            email_data = []
            for result in analysis_results:
                email_id = result['id']
                email = emails[email_id]
                
                if result['is_job_related']:
                    job_data = {
                        'company_name': result.get('company'),
                        'job_title': result.get('job_title'),
                        'application_status': result.get('status', 'unknown'),
                        'application_type': result.get('type', 'status_update')
                    }
                else:
                    job_data = {}
                
                email_data.append({
                    'email': email,
                    'is_job_related': result['is_job_related'],
                    'job_data': job_data
                })
            
            return email_data
            
        except Exception as e:
            self.logger.error(f"Error parsing Claude response: {str(e)}")
            self.logger.error(f"Claude response was: {claude_response}")
            # Fallback: mark all as non-job-related
            return [{'email': email, 'is_job_related': False, 'job_data': {}} for email in emails]
    
    
    def _save_email_backup(self, email):
        """Save email content to temp/job_emails directory"""
        try:
            filename = f"{email['gmail_id']}.json"
            filepath = os.path.join('temp', 'job_emails', filename)
            
            with open(filepath, 'w') as f:
                json.dump(email, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.warning(f"Failed to save email backup: {str(e)}")
    
    
    def _extract_company_from_sender(self, sender):
        """Extract company name from email sender"""
        if not sender:
            return None
        
        if '@' in sender:
            domain = sender.split('@')[-1].replace('>', '')
            company = domain.split('.')[0]
            return company.title() if company else None
        
        return None
    
    def _save_job_email(self, email, job_data, user_id):
        """Save job email to database"""
        try:
            date_received = None
            if email.get('time'):
                try:
                    date_received = datetime.strptime(email['time'], '%Y-%m-%d %H:%M:%S')
                except:
                    date_received = datetime.utcnow()
            else:
                date_received = datetime.utcnow()
            
            gmail_link = f"https://mail.google.com/mail/u/0/#inbox/{email['gmail_id']}" if email.get('gmail_id') else None
            
            job_email = JobEmail(
                gmail_id=email['gmail_id'],
                sender=email.get('sender', ''),
                subject=email.get('subject', ''),
                email_body=email.get('message', ''),
                company_name=job_data.get('company_name'),
                job_title=job_data.get('job_title'),
                application_status=job_data.get('application_status', 'unknown'),
                application_type=job_data.get('application_type', 'status_update'),
                date_received=date_received,
                user_id=user_id,
                is_read=False,
                gmail_link=gmail_link,
                extracted_metadata=job_data
            )
            
            self.db.session.add(job_email)
            self.db.session.commit()
            
        except Exception as e:
            self.logger.error(f"Error saving job email: {str(e)}")
            self.db.session.rollback()
            raise
    
    def get_job_emails(self, user_id: str, page: int = 1, per_page: int = 10, filters=None, sort_by='date_received', sort_order='desc'):
        """Get job emails for a user with pagination and filtering"""
        try:
            from sqlalchemy import desc, asc, or_
            
            query = self.db.session.query(JobEmail).filter(JobEmail.user_id == user_id)
            
            # Apply filters
            if filters:
                if filters.get('company_name'):
                    query = query.filter(JobEmail.company_name.ilike(f"%{filters['company_name']}%"))
                if filters.get('application_status'):
                    query = query.filter(JobEmail.application_status == filters['application_status'])
                if filters.get('application_type'):
                    query = query.filter(JobEmail.application_type == filters['application_type'])
                if filters.get('search'):
                    search_term = f"%{filters['search']}%"
                    query = query.filter(or_(
                        JobEmail.email_body.ilike(search_term),
                        JobEmail.subject.ilike(search_term),
                        JobEmail.sender.ilike(search_term),
                        JobEmail.company_name.ilike(search_term)
                    ))
                if filters.get('date_from'):
                    date_from = datetime.strptime(filters['date_from'], '%Y-%m-%d')
                    query = query.filter(JobEmail.date_received >= date_from)
                if filters.get('date_to'):
                    date_to = datetime.strptime(filters['date_to'], '%Y-%m-%d')
                    query = query.filter(JobEmail.date_received <= date_to)
            
            # Apply sorting
            if sort_order.lower() == 'desc':
                query = query.order_by(desc(getattr(JobEmail, sort_by)))
            else:
                query = query.order_by(asc(getattr(JobEmail, sort_by)))
            
            # Get total count
            total_count = query.count()
            
            # Apply pagination
            offset = (page - 1) * per_page
            job_emails = query.limit(per_page).offset(offset).all()
            
            emails_data = [{
                'id': str(email.id),
                'gmail_id': email.gmail_id,
                'sender_email': email.sender,
                'subject': email.subject,
                'email_body': email.email_body,
                'company_name': email.company_name,
                'job_title': email.job_title,
                'application_status': email.application_status,
                'application_type': email.application_type,
                'date_received': email.date_received.isoformat() if email.date_received else None,
                'is_read': email.is_read,
                'gmail_link': email.gmail_link,
                'extracted_metadata': email.extracted_metadata or {}
            } for email in job_emails]
            
            return {
                'emails': emails_data,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total_count,
                    'pages': (total_count + per_page - 1) // per_page
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting job emails: {str(e)}")
            raise
    
    def get_job_email_by_id(self, email_id: int, user_id: str):
        """Get specific job email by ID"""
        try:
            job_email = self.db.session.query(JobEmail).filter(
                and_(JobEmail.id == email_id, JobEmail.user_id == user_id)
            ).first()
            
            if not job_email:
                return None
            
            return {
                'id': job_email.id,
                'gmail_id': job_email.gmail_id,
                'sender': job_email.sender,
                'subject': job_email.subject,
                'email_body': job_email.email_body,
                'company_name': job_email.company_name,
                'job_title': job_email.job_title,
                'application_status': job_email.application_status,
                'application_type': job_email.application_type,
                'date_received': job_email.date_received.isoformat() if job_email.date_received else None,
                'created_at': job_email.created_at.isoformat() if job_email.created_at else None
            }
            
        except Exception as e:
            self.logger.error(f"Error getting job email by ID: {str(e)}")
            raise
    
    def update_email_details(self, email_id: int, user_id: str, updates: dict):
        """Update job email details"""
        try:
            job_email = self.db.session.query(JobEmail).filter(
                and_(JobEmail.id == email_id, JobEmail.user_id == user_id)
            ).first()
            
            if not job_email:
                return None
            
            # Update allowed fields
            allowed_fields = ['company_name', 'job_title', 'application_status', 'application_type', 'is_read']
            for field in allowed_fields:
                if field in updates:
                    setattr(job_email, field, updates[field])
            
            self.db.session.commit()
            
            return {
                'id': str(job_email.id),
                'gmail_id': job_email.gmail_id,
                'sender_email': job_email.sender,
                'subject': job_email.subject,
                'email_body': job_email.email_body,
                'company_name': job_email.company_name,
                'job_title': job_email.job_title,
                'application_status': job_email.application_status,
                'application_type': job_email.application_type,
                'date_received': job_email.date_received.isoformat() if job_email.date_received else None,
                'is_read': job_email.is_read,
                'gmail_link': job_email.gmail_link,
                'extracted_metadata': job_email.extracted_metadata or {}
            }
            
        except Exception as e:
            self.logger.error(f"Error updating job email: {str(e)}")
            self.db.session.rollback()
            raise
    
    def mark_email_as_read(self, email_id: int, user_id: str):
        """Mark email as read"""
        try:
            job_email = self.db.session.query(JobEmail).filter(
                and_(JobEmail.id == email_id, JobEmail.user_id == user_id)
            ).first()
            
            if not job_email:
                return False
            
            job_email.is_read = True
            self.db.session.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Error marking email as read: {str(e)}")
            self.db.session.rollback()
            raise
    
    def delete_email(self, email_id: int, user_id: str):
        """Delete job email"""
        try:
            job_email = self.db.session.query(JobEmail).filter(
                and_(JobEmail.id == email_id, JobEmail.user_id == user_id)
            ).first()
            
            if not job_email:
                return False
            
            self.db.session.delete(job_email)
            self.db.session.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting job email: {str(e)}")
            self.db.session.rollback()
            raise
    
    def get_email_stats(self, user_id: str):
        """Get email statistics for dashboard"""
        try:
            from sqlalchemy import func
            
            total_applications = self.db.session.query(JobEmail).filter(JobEmail.user_id == user_id).count()
            
            interviews_scheduled = self.db.session.query(JobEmail).filter(
                and_(JobEmail.user_id == user_id, JobEmail.application_status.like('%interview%'))
            ).count()
            
            offers_received = self.db.session.query(JobEmail).filter(
                and_(JobEmail.user_id == user_id, JobEmail.application_status == 'offer')
            ).count()
            
            rejections = self.db.session.query(JobEmail).filter(
                and_(JobEmail.user_id == user_id, JobEmail.application_status == 'rejected')
            ).count()
            
            pending_responses = self.db.session.query(JobEmail).filter(
                and_(JobEmail.user_id == user_id, JobEmail.application_status.in_(['applied', 'unknown']))
            ).count()
            
            return {
                'total_applications': total_applications,
                'interviews_scheduled': interviews_scheduled,
                'offers_received': offers_received,
                'rejections': rejections,
                'pending_responses': pending_responses
            }
            
        except Exception as e:
            self.logger.error(f"Error getting email stats: {str(e)}")
            raise
    
    def get_gmail_integration_status(self, user_id: str):
        """Get Gmail integration status for user"""
        try:
            user_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type=ServiceTypeEnum.Gmail.value
            ).first()
            
            if not user_token:
                return {
                    'is_connected': False,
                    'email': None,
                    'last_scan': None,
                    'token_expires_at': None
                }
            
            # Check if token is expired (expiry is timestamp in seconds)
            import time
            current_time = int(time.time())
            is_expired = user_token.expiry < current_time
            
            # Get last scan date (most recent job email for this user)
            last_email = self.db.session.query(JobEmail).filter(
                JobEmail.user_id == user_id
            ).order_by(JobEmail.created_at.desc()).first()
            
            last_scan = last_email.created_at.isoformat() if last_email else None
            
            return {
                'is_connected': not is_expired,
                'email': None,  # Could extract from token if available
                'last_scan': last_scan,
                'token_expires_at': datetime.fromtimestamp(user_token.expiry).isoformat() if user_token.expiry else None
            }
            
        except Exception as e:
            self.logger.error(f"Error getting Gmail integration status: {str(e)}")
            raise