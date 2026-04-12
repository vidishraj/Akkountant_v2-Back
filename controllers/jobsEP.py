import threading
import uuid
from datetime import datetime

from services.JobService import JobService
from services.tasks.scheduler import get_task_class, TASK_MAPPING
from utils.logger import Logger
from flask import request, jsonify, g


class JobsController:
    _running_jobs = {}
    _running_jobs_lock = threading.Lock()

    def __init__(self, flask_app=None):
        self.job_service = JobService()
        self.logger = Logger(__name__).get_logger()
        self.flask_app = flask_app
    
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

    def run_job_now(self):
        """Run a job immediately in a background thread, bypassing the time window."""
        try:
            data = request.get_json(force=True)
            title = data.get('title') if data else None
            if not title:
                return jsonify({'status': 'error', 'message': 'title is required'}), 400

            if title not in TASK_MAPPING:
                return jsonify({'status': 'error', 'message': f'Unknown job: {title}'}), 400

            # Concurrency guard: one run per title at a time
            with self._running_jobs_lock:
                for entry in self._running_jobs.values():
                    if entry['title'] == title and entry['status'] == 'running':
                        return jsonify({'status': 'error', 'message': f'{title} is already running'}), 409

            user_id = g.get('firebase_id', 'manual')
            job = self.job_service.create_running_job(title, user_id)
            run_id = uuid.uuid4().hex[:8]

            with self._running_jobs_lock:
                # Purge old entries (> 30 min)
                now = datetime.now()
                stale = [k for k, v in self._running_jobs.items()
                         if v['status'] != 'running' and (now - datetime.fromisoformat(v['started_at'])).total_seconds() > 1800]
                for k in stale:
                    del self._running_jobs[k]

                self._running_jobs[run_id] = {
                    'run_id': run_id,
                    'job_id': job.id,
                    'title': title,
                    'status': 'running',
                    'started_at': now.isoformat(),
                    'completed_at': None,
                    'duration_seconds': None,
                    'result': None,
                    'error': None,
                }

            thread = threading.Thread(
                target=self._execute_task,
                args=(run_id, job.id, title, self.flask_app, user_id),
                daemon=True,
            )
            thread.start()

            return jsonify({'run_id': run_id, 'job_id': job.id, 'status': 'running'}), 202

        except Exception as e:
            self.logger.error(f"Error in run_job_now: {str(e)}")
            return jsonify({'status': 'error', 'message': str(e)}), 500

    def _execute_task(self, run_id, job_id, title, flask_app, user_id):
        """Background thread: execute the task and update status."""
        started = datetime.now()
        try:
            with flask_app.app_context():
                g.firebase_id = user_id
                g.db = flask_app.extensions.get("sqlalchemy")

                task_class = get_task_class(title)
                task_instance = task_class(title, "High")
                result, status, _interval = task_instance.startTask()

                duration = (datetime.now() - started).total_seconds()
                final_status = status if status in ("Completed", "Failed") else "Completed"

                with self._running_jobs_lock:
                    self._running_jobs[run_id].update({
                        'status': final_status,
                        'result': result[:500] if result else None,
                        'completed_at': datetime.now().isoformat(),
                        'duration_seconds': round(duration, 1),
                    })

                self.job_service.update_job_result(job_id, result, final_status)
                self.logger.info(f"Run {run_id} ({title}): {final_status} in {duration:.1f}s")

        except Exception as e:
            duration = (datetime.now() - started).total_seconds()
            with self._running_jobs_lock:
                self._running_jobs[run_id].update({
                    'status': 'Failed',
                    'error': str(e)[:500],
                    'completed_at': datetime.now().isoformat(),
                    'duration_seconds': round(duration, 1),
                })
            try:
                with flask_app.app_context():
                    g.db = flask_app.extensions.get("sqlalchemy")
                    self.job_service.update_job_result(job_id, str(e)[:900], "Failed")
            except Exception:
                pass
            self.logger.error(f"Run {run_id} ({title}) failed: {e}")

    def get_run_status(self):
        """Poll the status of a running job."""
        run_id = request.args.get('run_id')
        if not run_id:
            return jsonify({'status': 'error', 'message': 'run_id is required'}), 400

        with self._running_jobs_lock:
            entry = self._running_jobs.get(run_id)

        if not entry:
            return jsonify({'status': 'error', 'message': 'Run not found'}), 404

        return jsonify(entry), 200