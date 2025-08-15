from flask import request, jsonify
from services.dashboardService import DashboardService
from utils.logger import Logger


class DashboardController:
    def __init__(self, dashboard_service):
        self.dashboard_service = dashboard_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def get_dashboard_analytics(self):
        try:
            analytics = self.dashboard_service.get_dashboard_analytics()
            return jsonify(analytics), 200

        except Exception as e:
            self.logger.error(f"Error in get_dashboard_analytics: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_earnings_by_date_range(self):
        try:
            start_date = request.args.get('startDate')
            end_date = request.args.get('endDate')

            if not start_date or not end_date:
                return jsonify({"error": "startDate and endDate parameters are required"}), 400

            earnings = self.dashboard_service.get_earnings_by_date_range(start_date, end_date)
            return jsonify(earnings), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in get_earnings_by_date_range: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500