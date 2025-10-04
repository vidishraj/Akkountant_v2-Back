from flask import g
from sqlalchemy import desc, asc, func, and_
from sqlalchemy.orm import sessionmaker
from models.Jobs import Job
from enums.TaskStatusEnum import JobStatus
from services.Base_Service import BaseService
from utils.logger import Logger
from datetime import datetime
import pytz

class JobService(BaseService):
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JobService, cls).__new__(cls)
            cls._instance.logger = Logger(__name__).get_logger()
        return cls._instance
    
    def __init__(self):
        super().__init__()
        self.ist_timezone = pytz.timezone('Asia/Kolkata')
        self.utc_timezone = pytz.UTC
    
    def get_job_summary(self):
        """Get summary of all unique job titles with their priorities"""
        try:
            # Get distinct job titles with their priorities
            unique_jobs = self.db.session.query(
                Job.title,
                Job.priority
            ).distinct(Job.title).all()
            
            summary = []
            for job_title, priority in unique_jobs:
                # Count jobs by status for this title
                status_counts = self.db.session.query(
                    Job.status,
                    func.count(Job.id).label('count')
                ).filter(Job.title == job_title).group_by(Job.status).all()
                
                status_dict = {status: count for status, count in status_counts}
                
                # Check if this job type is disabled (any job with 10+ failures)
                has_disabled_job = self.db.session.query(Job).filter(
                    and_(Job.title == job_title, Job.failures >= 10)
                ).first() is not None
                
                summary.append({
                    'title': job_title,
                    'priority': priority,
                    'pending_count': status_dict.get(JobStatus.PENDING.value, 0),
                    'overdue_count': status_dict.get(JobStatus.OVERDUE.value, 0),
                    'completed_count': status_dict.get(JobStatus.COMPLETED.value, 0),
                    'failed_count': status_dict.get(JobStatus.FAILED.value, 0),
                    'is_disabled': has_disabled_job
                })
            
            return {
                'status': 'success',
                'data': summary
            }
            
        except Exception as e:
            self.logger.error(f"Error getting job summary: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to get job summary: {str(e)}'
            }
    
    def get_jobs_by_title_and_status(self, title, status, page=1, per_page=10):
        """Get paginated jobs for a specific title and status"""
        try:
            # Validate status
            valid_statuses = [s.value for s in JobStatus]
            if status not in valid_statuses:
                return {
                    'status': 'error',
                    'message': f'Invalid status. Must be one of: {valid_statuses}'
                }
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Query jobs with pagination
            query = self.db.session.query(Job).filter(
                and_(Job.title == title, Job.status == status)
            ).order_by(desc(Job.due_date))
            
            total_count = query.count()
            jobs = query.offset(offset).limit(per_page).all()
            
            # Check if this job type is disabled
            job_type_disabled = self.db.session.query(Job).filter(
                and_(Job.title == title, Job.failures >= 10)
            ).first() is not None
            
            # Convert to dict format
            job_list = []
            for job in jobs:
                # Convert IST due_date to UTC for frontend
                utc_due_date = None
                if job.due_date:
                    # Assume stored datetime is IST, convert to UTC
                    ist_date = self.ist_timezone.localize(job.due_date)
                    utc_date = ist_date.astimezone(self.utc_timezone)
                    utc_due_date = utc_date.isoformat()
                
                job_list.append({
                    'id': job.id,
                    'title': job.title,
                    'result': job.result,
                    'priority': job.priority,
                    'status': job.status,
                    'due_date': utc_due_date,
                    'failures': job.failures,
                    'user_id': job.user_id,
                    'job_type_disabled': job_type_disabled  # Indicate if this job TYPE is disabled
                })
            
            # Calculate pagination info
            total_pages = (total_count + per_page - 1) // per_page
            has_next = page < total_pages
            has_prev = page > 1
            
            return {
                'status': 'success',
                'data': {
                    'jobs': job_list,
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total': total_count,
                        'pages': total_pages,
                        'has_next': has_next,
                        'has_prev': has_prev
                    }
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting jobs by title and status: {str(e)}")
            return {
                'status': 'error',
                'message': f'Failed to get jobs: {str(e)}'
            }
    
    def cancel_job(self, job_id):
        """Cancel a pending or overdue job"""
        try:
            job = self.db.session.query(Job).filter(Job.id == job_id).first()
            if not job:
                return {
                    'status': 'error',
                    'message': 'Job not found'
                }
            
            # Only allow cancellation of pending or overdue jobs
            if job.status not in [JobStatus.PENDING.value, JobStatus.OVERDUE.value]:
                return {
                    'status': 'error',
                    'message': f'Cannot cancel job with status: {job.status}. Only pending or overdue jobs can be cancelled.'
                }
            
            # Delete the job (equivalent to cancellation)
            self.db.session.delete(job)
            self.db.session.commit()
            
            return {
                'status': 'success',
                'message': f'Job {job_id} has been cancelled'
            }
            
        except Exception as e:
            self.logger.error(f"Error cancelling job: {str(e)}")
            self.db.session.rollback()
            return {
                'status': 'error',
                'message': f'Failed to cancel job: {str(e)}'
            }
    
    def cancel_jobs_bulk(self, job_ids):
        """Cancel multiple pending or overdue jobs"""
        try:
            if not job_ids or not isinstance(job_ids, list):
                return {
                    'status': 'error',
                    'message': 'job_ids must be a non-empty list'
                }
            
            # Get all jobs with the provided IDs
            jobs = self.db.session.query(Job).filter(Job.id.in_(job_ids)).all()
            
            if not jobs:
                return {
                    'status': 'error',
                    'message': 'No jobs found with provided IDs'
                }
            
            cancelled_count = 0
            errors = []
            
            for job in jobs:
                if job.status in [JobStatus.PENDING.value, JobStatus.OVERDUE.value]:
                    self.db.session.delete(job)
                    cancelled_count += 1
                else:
                    errors.append(f'Job {job.id} cannot be cancelled (status: {job.status})')
            
            self.db.session.commit()
            
            result = {
                'status': 'success',
                'message': f'Successfully cancelled {cancelled_count} jobs',
                'cancelled_count': cancelled_count
            }
            
            if errors:
                result['errors'] = errors
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in bulk cancel jobs: {str(e)}")
            self.db.session.rollback()
            return {
                'status': 'error',
                'message': f'Failed to cancel jobs: {str(e)}'
            }