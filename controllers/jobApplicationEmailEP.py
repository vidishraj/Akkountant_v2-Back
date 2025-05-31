from services.jobApplicationEmailService import JobApplicationEmailService
from utils.logger import Logger
from flask import request, jsonify

class JobApplicationEmailController:

    def __init__(self, jobApplicationEmailService=None):
        self.JobApplicationEmailService = jobApplicationEmailService or JobApplicationEmailService()
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def processEmails(self):
        """
        Expects a JSON list of emails: [{date, subject, body}, ...]
        """
        email_list = request.get_json(force=True)
        self.logger.info(f"Processing {len(email_list)} emails for job applications")
        self.JobApplicationEmailService.process_emails(email_list)
        return jsonify({'status': 'success'}), 201

    @Logger.standardLogger
    def fetchJobApplications(self):
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        result = self.JobApplicationEmailService.get_paginated(page, per_page)
        items = [{
            'id': item.id,
            'email_date': item.email_date.isoformat(),
            'employer': item.employer,
            'role': item.role,
            'email_subject': item.email_subject,
            'email_body': item.email_body
        } for item in result['items']]
        return jsonify({
            'items': items,
            'total': result['total'],
            'page': result['page'],
            'per_page': result['per_page'],
            'pages': result['pages']
        }), 200

# Register endpoints in your app.py
def register_job_application_email_ep(app):
    controller = JobApplicationEmailController()
    app.add_url_rule(
        '/job-applications/process-emails',
        view_func=controller.processEmails,
        methods=['POST']
    )
    app.add_url_rule(
        '/job-applications/',
        view_func=controller.fetchJobApplications,
        methods=['GET']
    )