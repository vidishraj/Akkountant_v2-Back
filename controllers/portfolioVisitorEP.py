from flask import request, jsonify, g
from services.portfolioVisitorService import PortfolioVisitorService
from utils.logger import Logger


class PortfolioVisitorController:

    def __init__(self, service: PortfolioVisitorService):
        self.service = service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def get_visitors(self):
        """GET /portfolio/visitors?page=1&per_page=50"""
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        per_page = min(per_page, 100)  # Cap at 100
        data = self.service.get_visitors(page, per_page)
        return jsonify(data)

    @Logger.standardLogger
    def get_stats(self):
        """GET /portfolio/stats"""
        data = self.service.get_stats()
        return jsonify(data)
