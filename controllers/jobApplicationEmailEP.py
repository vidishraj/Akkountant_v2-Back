from services.jobApplicationEmailService import JobApplicationEmailService
from services.transactionsService import TransactionService
from utils.logger import Logger
from flask import request, jsonify

class JobApplicationEmailController:

    def __init__(self, transactionService: TransactionService):
        self.JobApplicationEmailService = JobApplicationEmailService()
        self.TransactionService = transactionService 
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def processEmails(self):
        """
        Process emails for job applications in a date range.
        Query parameters:
            - dateFrom: Start date (YYYY/MM/DD)
            - dateTo: End date (YYYY/MM/DD)
            - userId: User ID
            - jobsOnly: If true, only process for job applications (optional)
        """
        try:
            date_from = request.args.get('dateFrom')
            date_to = request.args.get('dateTo')
            user_id = request.headers.get("X-Firebase-ID")
            if not all([date_from, date_to, user_id]):
                return jsonify({
                    'status': 'error',
                    'message': 'dateFrom, dateTo, and userId are required'
                }), 400
            
            # Get emails from Gmail
            
            # Process emails
            result = self.TransactionService.readTransactionFromMail(date_from, date_to, user_id, jobsOnly=True)
            
            return jsonify({
                'status': 'success',
                'data': result
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error in processEmails: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to process emails: {str(e)}'
            }), 500

    @Logger.standardLogger
    def fetchJobApplications(self):
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        result = self.JobApplicationEmailService.get_paginated_job_emails(page, per_page)

        return jsonify(result), 200

    @Logger.standardLogger
    def updateJobApplication(self):
        """
        Update job application email records.
        Expects a JSON array of updates: [
            {
                "jobId": "1",
                "field": "verdict",
                "value": "2"
            },
            {
                "jobId": "1",
                "field": "employer",
                "value": "Hello"
            }
        ]
        """
        try:
            updates = request.get_json(force=True)['updates']
            
            if not isinstance(updates, list):
                return jsonify({
                    'status': 'error',
                    'message': 'Request body must be an array of updates'
                }), 400
            
            if not updates:
                return jsonify({
                    'status': 'error',
                    'message': 'Updates array cannot be empty'
                }), 400
            
            # Group updates by jobId
            grouped_updates = {}
            for update in updates:
                job_id = update.get('jobId')
                field = update.get('field')
                value = update.get('value')
                
                if not all([job_id, field, value]):
                    return jsonify({
                        'status': 'error',
                        'message': 'Each update must contain jobId, field, and value'
                    }), 400
                
                if job_id not in grouped_updates:
                    grouped_updates[job_id] = {}
                grouped_updates[job_id][field] = value
            
            # Process updates for each job
            results = []
            for job_id, updates_dict in grouped_updates.items():
                result = self.JobApplicationEmailService.update_job_application(int(job_id), updates_dict)
                results.append({
                    'jobId': job_id,
                    'status': result['status'],
                    'message': result.get('message', 'Success'),
                    'data': result.get('data')
                })
            
            # Check if any update failed
            has_errors = any(r['status'] == 'error' for r in results)
            
            return jsonify({
                'status': 'error' if has_errors else 'success',
                'results': results
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error in updateJobApplication: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to update job applications: {str(e)}'
            }), 500
