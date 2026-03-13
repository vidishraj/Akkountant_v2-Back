from services.JobService import JobService
from utils.logger import Logger
from flask import request, jsonify

class JobsController:
    
    def __init__(self):
        self.job_service = JobService()
        self.logger = Logger(__name__).get_logger()
    
    @Logger.standardLogger
    def get_jobs_summary(self):
        """
        Get summary of all job titles with their status counts.
        Returns job titles with priority and counts for pending, overdue, completed, and failed jobs.
        """
        try:
            result = self.job_service.get_job_summary()
            
            if result['status'] == 'success':
                return jsonify({
                    'status': 'success',
                    'data': result['data']
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'message': result['message']
                }), 500
                
        except Exception as e:
            self.logger.error(f"Error in get_jobs_summary: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get jobs summary: {str(e)}'
            }), 500
    
    @Logger.standardLogger
    def get_jobs_by_title_status(self):
        """
        Get paginated jobs for a specific title and status.
        Query parameters:
            - title: Job title (required)
            - status: Job status (required) - one of: Pending, Overdue, Completed, Failed
            - page: Page number (default: 1)
            - per_page: Items per page (default: 10, max: 100)
        
        Note: Each job response includes 'job_type_disabled' flag indicating if this job type
        is disabled (has any job with 10+ failures) and won't be rescheduled.
        """
        try:
            title = request.args.get('title')
            status = request.args.get('status')
            page = int(request.args.get('page', 1))
            per_page = min(int(request.args.get('per_page', 10)), 100)  # Max 100 per page
            
            if not title:
                return jsonify({
                    'status': 'error',
                    'message': 'title parameter is required'
                }), 400
            
            if not status:
                return jsonify({
                    'status': 'error',
                    'message': 'status parameter is required'
                }), 400
            
            if page < 1:
                return jsonify({
                    'status': 'error',
                    'message': 'page must be >= 1'
                }), 400
            
            if per_page < 1:
                return jsonify({
                    'status': 'error',
                    'message': 'per_page must be >= 1'
                }), 400
            
            result = self.job_service.get_jobs_by_title_and_status(title, status, page, per_page)
            
            if result['status'] == 'success':
                return jsonify({
                    'status': 'success',
                    'data': result['data']
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'message': result['message']
                }), 400
                
        except ValueError as ve:
            return jsonify({
                'status': 'error',
                'message': f'Invalid parameter format: {str(ve)}'
            }), 400
        except Exception as e:
            self.logger.error(f"Error in get_jobs_by_title_status: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get jobs: {str(e)}'
            }), 500
    
    @Logger.standardLogger
    def get_jobs_daily_history(self):
        """Get day-by-day status breakdown for all job types."""
        try:
            days = int(request.args.get('days', 90))
            result = self.job_service.get_job_daily_history(days)

            if result['status'] == 'success':
                return jsonify({
                    'status': 'success',
                    'data': result['data']
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'message': result['message']
                }), 500

        except Exception as e:
            self.logger.error(f"Error in get_jobs_daily_history: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get daily history: {str(e)}'
            }), 500

    @Logger.standardLogger
    def cancel_job(self, job_id):
        """
        Cancel a single pending or overdue job.
        Path parameter:
            - job_id: ID of the job to cancel
        """
        try:
            if not job_id:
                return jsonify({
                    'status': 'error',
                    'message': 'job_id is required'
                }), 400
            
            try:
                job_id = int(job_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'job_id must be a valid integer'
                }), 400
            
            result = self.job_service.cancel_job(job_id)
            
            if result['status'] == 'success':
                return jsonify({
                    'status': 'success',
                    'message': result['message']
                }), 200
            else:
                status_code = 404 if 'not found' in result['message'].lower() else 400
                return jsonify({
                    'status': 'error',
                    'message': result['message']
                }), status_code
                
        except Exception as e:
            self.logger.error(f"Error in cancel_job: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to cancel job: {str(e)}'
            }), 500
    
    @Logger.standardLogger
    def cancel_jobs_bulk(self):
        """
        Cancel multiple pending or overdue jobs.
        Request body:
            {
                "job_ids": [1, 2, 3, ...]
            }
        """
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({
                    'status': 'error',
                    'message': 'Request body is required'
                }), 400
            
            job_ids = data.get('job_ids')
            
            if not job_ids:
                return jsonify({
                    'status': 'error',
                    'message': 'job_ids array is required'
                }), 400
            
            if not isinstance(job_ids, list):
                return jsonify({
                    'status': 'error',
                    'message': 'job_ids must be an array'
                }), 400
            
            # Validate all job_ids are integers
            try:
                job_ids = [int(job_id) for job_id in job_ids]
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'All job_ids must be valid integers'
                }), 400
            
            if len(job_ids) > 100:  # Limit bulk operations
                return jsonify({
                    'status': 'error',
                    'message': 'Cannot cancel more than 100 jobs at once'
                }), 400
            
            result = self.job_service.cancel_jobs_bulk(job_ids)
            
            if result['status'] == 'success':
                return jsonify({
                    'status': 'success',
                    'message': result['message'],
                    'cancelled_count': result['cancelled_count'],
                    'errors': result.get('errors', [])
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'message': result['message']
                }), 400
                
        except Exception as e:
            self.logger.error(f"Error in cancel_jobs_bulk: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to cancel jobs: {str(e)}'
            }), 500