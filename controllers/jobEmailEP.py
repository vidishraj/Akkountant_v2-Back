from services.jobEmailService import JobEmailService
from utils.logger import Logger
from flask import request, jsonify


class JobEmailController:
    
    def __init__(self, job_email_service):
        self.job_email_service = job_email_service
        self.logger = Logger(__name__).get_logger()
    
    @Logger.standardLogger
    def scan_emails(self):
        """Endpoint to scan emails for job applications"""
        try:
            data = request.get_json(force=True) if request.is_json else {}
            user_id = request.headers.get('X-Firebase-ID')
            
            # Handle date range from frontend - your format: {date_from: "2025-11-22", date_to: "2025-11-23"}
            date_from = data.get('date_from')
            date_to = data.get('date_to')
            
            self.logger.debug(f"Received date range: {date_from} to {date_to}")
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            result = self.job_email_service.scan_emails_for_jobs(user_id, date_from, date_to)
            
            from datetime import datetime
            return jsonify({
                "status": "success",
                "data": {
                    "processed_count": result['processed'],
                    "new_emails_count": result['new'],
                    "updated_emails_count": 0,  # Not implemented yet
                    "scan_date": datetime.now().isoformat()
                }
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error scanning emails: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def get_job_emails(self):
        """Endpoint to get job emails with pagination and filtering"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            # Get query parameters
            page = int(request.args.get('page', 1))
            per_page = int(request.args.get('per_page', 10))
            sort_by = request.args.get('sort_by', 'date_received')
            sort_order = request.args.get('sort_order', 'desc')
            
            # Build filters
            filters = {}
            for key in ['company_name', 'application_status', 'application_type', 'search', 'date_from', 'date_to']:
                value = request.args.get(key)
                if value:
                    filters[key] = value
            
            result = self.job_email_service.get_job_emails(
                user_id=user_id,
                page=page,
                per_page=per_page,
                filters=filters or None,
                sort_by=sort_by,
                sort_order=sort_order
            )
            
            return jsonify({
                "status": "success",
                "data": result
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error getting job emails: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def get_job_email_details(self, email_id):
        """Endpoint to get specific job email details"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            job_email = self.job_email_service.get_job_email_by_id(int(email_id), user_id)
            
            if not job_email:
                return jsonify({"status": "error", "message": "Email not found"}), 404
            
            return jsonify({
                "status": "success",
                "data": job_email
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error getting job email details: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def update_email_details(self, email_id):
        """Endpoint to update job email details"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            data = request.get_json(force=True)
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            updated_email = self.job_email_service.update_email_details(int(email_id), user_id, data)
            
            if not updated_email:
                return jsonify({"status": "error", "message": "Email not found"}), 404
            
            return jsonify({
                "status": "success",
                "data": updated_email
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error updating job email: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def mark_email_as_read(self, email_id):
        """Endpoint to mark email as read"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            success = self.job_email_service.mark_email_as_read(int(email_id), user_id)
            
            if not success:
                return jsonify({"status": "error", "message": "Email not found"}), 404
            
            return jsonify({"status": "success"}), 200
            
        except Exception as e:
            self.logger.error(f"Error marking email as read: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def delete_email(self, email_id):
        """Endpoint to delete job email"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            success = self.job_email_service.delete_email(int(email_id), user_id)
            
            if not success:
                return jsonify({"status": "error", "message": "Email not found"}), 404
            
            return jsonify({"status": "success"}), 200
            
        except Exception as e:
            self.logger.error(f"Error deleting job email: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def get_email_stats(self):
        """Endpoint to get email statistics"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            stats = self.job_email_service.get_email_stats(user_id)
            
            return jsonify({
                "status": "success",
                "data": stats
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error getting email stats: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def get_gmail_status(self):
        """Endpoint to get Gmail integration status"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            status = self.job_email_service.get_gmail_integration_status(user_id)
            
            return jsonify({
                "status": "success",
                "data": status
            }), 200
            
        except Exception as e:
            self.logger.error(f"Error getting Gmail status: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    
    @Logger.standardLogger
    def refresh_gmail_token(self):
        """Endpoint to refresh Gmail token"""
        try:
            user_id = request.headers.get('X-Firebase-ID')
            
            if not user_id:
                return jsonify({"error": "X-Firebase-ID header required"}), 400
            
            # This is a placeholder - you'll need to implement actual token refresh
            return jsonify({"status": "success"}), 200
            
        except Exception as e:
            self.logger.error(f"Error refreshing Gmail token: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500