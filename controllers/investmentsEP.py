import os
from datetime import datetime

from werkzeug.utils import secure_filename

from enums.EPGEnum import EPGEnum
from enums.MsnEnum import MSNENUM
from services.InvestmentService import InvestmentService
from services.KiteService import KiteAuthError
from utils.logger import Logger
from flask import request, jsonify, g, Response


# ak-k9d8: result of the delete-all confirm check.
CONFIRM_OK = "ok"
CONFIRM_MISSING = "missing"
CONFIRM_MISMATCH = "mismatch"


def check_delete_all_confirm(service_type_param, body, raw_body_present=False):
    """ak-k9d8: defence in depth for DELETE /deleteAllInvestments against
    stray or replayed calls. It is NOT a server-side type-to-confirm (the
    human confirmation is the FE dialog). The client must echo the target
    asset as body {"confirm": "<serviceType>"}, compared EXACTLY
    (case-sensitive, enum-name form: Stocks / Mutual_Funds / NPS / EPF /
    PF / Gold) with the serviceType query param.

    ``body`` is the parsed JSON (None if there was none or it did not
    parse); ``raw_body_present`` says whether the request carried any body
    bytes at all.

    Returns CONFIRM_OK | CONFIRM_MISSING | CONFIRM_MISMATCH. The endpoint
    rejects both MISSING and MISMATCH (ak-b8em: confirm is REQUIRED; the
    one-deploy-cycle transition that allowed MISSING is over).
    ak-b8em: a body that is present but unparsable (or sent with a
    non-JSON content type) is a MISMATCH, never MISSING.
    """
    if body is None:
        return CONFIRM_MISMATCH if raw_body_present else CONFIRM_MISSING
    if not isinstance(body, dict):
        return CONFIRM_MISMATCH
    if "confirm" not in body:
        return CONFIRM_MISSING
    confirm = body.get("confirm")
    if isinstance(confirm, str) and confirm == service_type_param:
        return CONFIRM_OK
    return CONFIRM_MISMATCH



def _bad_request(message):
    """ak-xfrt: a 400 error as a single Response object (never a tuple,
    so it can't be mistaken for getUserIdServiceType's success value)."""
    resp = jsonify({"error": message})
    resp.status_code = 400
    return resp

class InvestmentController:
    InvestmentService: InvestmentService

    def __init__(self, investmentService):
        self.InvestmentService = investmentService
        self.logger = Logger(__name__).get_logger()

    @staticmethod
    def getUserIdServiceType(service_type_param):
        """Return ``(user_id, service_type)`` on success, or a Flask
        ``Response`` (status 400) on invalid input.

        ak-xfrt: errors used to be returned as a ``(response, 400)`` TUPLE
        while every caller tested ``not isinstance(params, tuple)``, so
        the error tuple was unpacked as ``(user_id, service_type)`` and
        the request crashed with a 500. Contract now: callers test
        ``isinstance(params, Response)`` for the error case.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return _bad_request("User ID not found")
        # Validate and get the serviceType from query parameter
        if not service_type_param or (service_type_param not in MSNENUM.__members__ and
                                      service_type_param not in EPGEnum.__members__):
            return _bad_request("Invalid or missing serviceType parameter")
        try:
            service_type = MSNENUM[service_type_param]
        except KeyError:
            service_type = EPGEnum[service_type_param]
        return user_id, service_type

    @Logger.standardLogger
    def fetchSecurityList(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchAllSecurities(service_type)

    @Logger.standardLogger
    def fetchSecurityRate(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        schemeCode = request.args.get('schemeCode')
        params = self.getUserIdServiceType(service_type_param)
        if not schemeCode:
            return jsonify({"error": "Scheme code missing"}), 400
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}, scheme: {schemeCode}")
        return self.InvestmentService.fetchSecuritySchemeRate(service_type.value, schemeCode)

    @Logger.standardLogger
    def process_file_upload(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")

        # Check if a file is in the request
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Save the file to the /tmp directory
        file_path = None
        try:
            tmp_folder = os.path.join(os.getcwd(), 'tmp')
            os.makedirs(tmp_folder, exist_ok=True)
            file_path = os.path.join(tmp_folder, secure_filename(file.filename))
            file.save(file_path)

            self.logger.info(f"File saved to: {file_path}")

            insertedDetails = self.InvestmentService.processFiles(service_type, file_path, user_id)
        finally:
            if file_path is not None:
                self.logger.info("Temp file deleted successfully.")
                os.remove(file_path)
        return jsonify({"Details": insertedDetails}), 200

    @Logger.standardLogger
    def fetchSummary(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchSummary(service_type.value, user_id)

    @Logger.standardLogger
    def fetchSecurityTransactions(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return jsonify(self.InvestmentService.fetchSecurityTransactions(service_type.value, user_id)), 200

    @Logger.standardLogger
    def fetchUserSecurities(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchUserSecurities(service_type.value, user_id)

    @Logger.standardLogger
    def fetchHistory(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchHistory(service_type, user_id)

    @Logger.standardLogger
    def fetchActiveSecurities(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchActiveSecurities(service_type, user_id)

    @Logger.standardLogger
    def insertSecurityTransaction(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        data, error = self.validate_security_transaction(service_type)
        if error:
            return error  # Return the validation error response
        return self.InvestmentService.insertSecurityPurchase(service_type, user_id, data)

    @Logger.standardLogger
    def fetchCompleteDataForEPG(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchActiveSecurities(service_type, user_id)

    @staticmethod
    def validate_security_transaction(service_type):
        """
        Validates the JSON body for the insertSecurityTransaction request.

        Args:
            service_type (str): The service type, e.g., 'Mutual_Funds'.

        Returns:
            tuple: A tuple with (validated_data, error_message).
                   If valid, error_message will be None, else it will contain the error details.
        """
        try:
            # Parse JSON body
            data = request.get_json()

            # Check if the JSON body is provided
            if not data:
                return None, (jsonify({"error": "Request body must be JSON"}), 400)

            # Required keys
            required_keys = ["date", "description", "amount"]
            required_keysMF = ["date", "amount", "quantity", "schemeCode"]
            # Ensure all required fields are present
            if service_type != MSNENUM.Mutual_Funds:
                missing_keys = [key for key in required_keys if key not in data]
            else:
                missing_keys = [key for key in required_keysMF if key not in data]

            if missing_keys:
                return None, (jsonify({"error": f"Missing keys: {', '.join(missing_keys)}"}), 400)

            # Validate 'date' field
            try:
                if not data["date"]:
                    return None, (jsonify({"error": "Date cannot be empty"}), 400)
                datetime.strptime(data["date"], "%d-%m-%Y")
            except (ValueError, TypeError):
                return None, (jsonify({"error": "Invalid date format. Use dd-mm-YYYY"}), 400)

            # Validate 'deposit' and 'amount' fields
            if service_type != MSNENUM.Mutual_Funds:
                if not isinstance(data["description"], str) or not data["description"]:
                    return None, (jsonify({"error": "Invalid 'description'. It must be a non-empty string"}), 400)
                if not isinstance(data["amount"], (int, float)) or data["amount"] <= 0:
                    return None, (jsonify({"error": "Invalid 'amount'. It must be a positive number"}), 400)

            # All validations passed
            return data, None

        except Exception as e:
            return None, (jsonify({"error": f"Unexpected error: {str(e)}"}), 500)

    @Logger.standardLogger
    def fetchRateForEPG(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchRateForEPG(service_type)

    @Logger.standardLogger
    def fetchRealizedPnL(self):
        """Fetch realized profit/loss and historical closed trades"""
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return jsonify(self.InvestmentService.fetchRealizedPnL(service_type.value, user_id)), 200

    @Logger.standardLogger
    def fetchFOSummary(self):
        """Fetch F&O P&L summary"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        return jsonify(self.InvestmentService.fetchFOSummary(user_id)), 200

    @Logger.standardLogger
    def fetchFOTrades(self):
        """Fetch all individual F&O trades"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        return jsonify(self.InvestmentService.fetchFOTrades(user_id)), 200

    @Logger.standardLogger
    def deleteAllInvestments(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # ak-k9d8: the body must echo the target asset (see
        # check_delete_all_confirm). Checked BEFORE anything is deleted.
        # ak-b8em: raw body bytes tell an unparsable body (MISMATCH) apart
        # from no body at all (MISSING).
        verdict = check_delete_all_confirm(
            service_type_param, request.get_json(silent=True),
            raw_body_present=bool(request.get_data(cache=True)),
        )
        if verdict == CONFIRM_MISMATCH:
            self.logger.warning(
                f"deleteAllInvestments REJECTED: confirm does not match "
                f"serviceType={service_type_param}"
            )
            return jsonify({
                "error": "confirm must equal the serviceType being deleted",
                "error_code": "DELETE_ALL_CONFIRM_MISMATCH",
            }), 400
        if verdict != CONFIRM_OK:
            # ak-b8em: confirm is REQUIRED (the k9d8 transition that let a
            # missing confirm through is over). Nothing is deleted.
            self.logger.warning(
                f"deleteAllInvestments REJECTED: no confirm "
                f"(serviceType={service_type_param})"
            )
            return jsonify({
                "error": "confirm is required and must equal the serviceType being deleted",
                "error_code": "DELETE_ALL_CONFIRM_REQUIRED",
            }), 400
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.deleteAll(service_type, user_id)

    @Logger.standardLogger
    def deleteSingleRecord(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if isinstance(params, Response):
            # Error has happened during validation
            return params
        user_id, service_type = params
        buyId = request.args.get('buyId')
        if buyId is None:
            return jsonify({"Error": "BuyID is missing"}), 406
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.deleteSingleRecord(service_type, buyId)

    @Logger.standardLogger
    def getJobsTable(self):
        # Get page and filter/sort parameters
        page = request.args.get('page')
        if page is None:
            return jsonify({"Error": "Page is missing"}), 406
        
        # Extract filter parameters
        filters = {
            'title': request.args.get('title'),
            'status': request.args.get('status'),
            'priority': request.args.get('priority'),
            'user_id': request.args.get('user_id'),
            'min_failures': request.args.get('min_failures'),
            'max_failures': request.args.get('max_failures')
        }
        
        # Extract sort parameters
        sort_by = request.args.get('sort_by', 'due_date')  # Default sort by due_date
        sort_order = request.args.get('sort_order', 'desc')  # Default descending
        
        return jsonify(self.InvestmentService.getJobsTable(page, filters, sort_by, sort_order)), 200

    @Logger.standardLogger
    def setJobs(self):
        # Get user ID from context.
        # ak-c8l: verified uid from g — NOT the raw header (that
        # read bypassed JWT verify and reinstated the IDOR
        # surface this bead closes).
        jobId = request.args.get('jobId')
        userId = g.get("firebase_id")
        if jobId is None:
            return jsonify({"Error": "JobID is missing"}), 406
        return self.InvestmentService.setJobsTable(jobId, userId)

    @Logger.standardLogger
    def fetchTimeStamps(self):
        return jsonify(self.InvestmentService.getFileTimeStamps()), 200

    @Logger.standardLogger
    def fetchInvestmentEmails(self):
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        category = request.args.get('category')
        service_type = request.args.get('serviceType')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 50))
        categories_param = request.args.get('categories')
        categories = categories_param.split(',') if categories_param else None
        return jsonify(self.InvestmentService.fetchInvestmentEmails(user_id, category, page, page_size, service_type, categories)), 200

    @Logger.standardLogger
    def fetchPortfolioHistory(self):
        """ak-4o41 Bundle 1 (supersedes ak-874): N-day portfolio trend.

        Returns per-day total portfolio value aggregated from the
        Kite historical-candle cache. See
        ``InvestmentService.getPortfolioHistory`` docstring for the
        constant-composition semantic + edge cases.

        Query params:
          * ``days``: int, defaults to 30; clamped to [7, 365] by
            the service layer.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        days_param = request.args.get('days')
        try:
            result = self.InvestmentService.getPortfolioHistory(
                user_id, days=days_param
            )
            return jsonify(result), 200
        except Exception as e:
            self.logger.error(f"Error fetching portfolio history: {str(e)}")
            return jsonify({"error": "Failed to fetch portfolio history"}), 500

    @Logger.standardLogger
    def fetchStockCandles(self, security_code):
        """ak-ue7l (2026-11-22): per-stock OHLC candles from the
        local ``stockHistoricalCandles`` cache (populated daily by
        BackfillHistoricalCandlesTask / ak-4o41).

        Serves from the local cache — NO Kite broker call per
        request. That's the whole point of ak-4o41: a chart load
        that re-hit Kite would burn the 3 req/s historical rate
        limit and 401 on the daily 6 AM IST token-expiry.

        Path param:
          * ``security_code``: trading symbol (e.g. 'RELIANCE').

        Query param:
          * ``range``: window string — ``'7d'`` / ``'30d'`` /
            ``'90d'`` / ``'1y'`` / ``'365d'`` etc. Default
            ``'90d'``. Clamped to [1, 730] days by the service.

        Response:
          ``{"candles": [{date, open, high, low, close, volume}, ...]}``
          sorted by date ascending. Empty list on unresolved
          symbol or no candles in the window (soft-fail; FE
          renders empty-state chart rather than crashing).

        READ-ONLY per bead — no destructive paths.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        range_param = request.args.get('range')
        try:
            candles = self.InvestmentService.fetchStockCandles(
                security_code, range_str=range_param,
            )
            return jsonify({
                "candles": candles,
                "security_code": security_code,
            }), 200
        except Exception as e:
            self.logger.error(
                f"Error fetching stock candles for "
                f"{security_code!r}: {str(e)}"
            )
            return jsonify({"error": "Failed to fetch candles"}), 500

    @Logger.standardLogger
    def fetchInvestmentSnapshots(self):
        """Fetch historical investment snapshots for growth charts."""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        date_from = request.args.get('dateFrom')
        date_to = request.args.get('dateTo')
        investment_type = request.args.get('investmentType')
        result = self.InvestmentService.getInvestmentSnapshots(
            user_id, date_from, date_to, investment_type
        )
        return jsonify({"snapshots": result}), 200

    @Logger.standardLogger
    def fetchEmailBody(self):
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        gmail_id = request.args.get('gmailId')
        if not gmail_id:
            return jsonify({"error": "gmailId parameter is required"}), 400
        try:
            result = self.InvestmentService.fetchEmailBody(user_id, gmail_id)
            if isinstance(result, tuple):
                return jsonify(result[0]), result[1]
            return jsonify(result), 200
        except Exception as e:
            self.logger.error(f"Error fetching email body: {str(e)}")
            return jsonify({"error": "Failed to fetch email body"}), 500

    def _kiteReconnectResponse(self, exc):
        """401 payload telling the UI to walk the Kite login flow again.

        Kite access tokens expire at 6 AM IST every day and there is no
        programmatic renewal available to us, so this is an expected daily
        state, not a server fault -- it must not surface as a 500.
        """
        self.logger.warning(f"Kite reconnect required: {str(exc)}")
        payload = {
            "error": str(exc),
            "reconnect_required": True,
            "kite_connected": False,
        }
        try:
            payload["login_url"] = self.InvestmentService.getKiteLoginUrl()
        except Exception as urlErr:
            self.logger.error(f"Could not build Kite login URL: {str(urlErr)}")
            payload["login_url"] = None
        return jsonify(payload), 401

    @Logger.standardLogger
    def fetchKiteHoldings(self):
        """Fetch holdings from Kite Connect API"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            holdings = self.InvestmentService.fetchKiteHoldings(user_id)
            return jsonify({"holdings": holdings}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite holdings: {str(e)}")
            return jsonify({"error": "Failed to fetch holdings"}), 500

    @Logger.standardLogger
    def fetchKitePositions(self):
        """Fetch positions from Kite Connect API"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            positions = self.InvestmentService.fetchKitePositions(user_id)
            return jsonify({"positions": positions}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite positions: {str(e)}")
            return jsonify({"error": "Failed to fetch positions"}), 500

    @Logger.standardLogger
    def fetchKiteMargins(self):
        """ak-zai2 Bundle 2 (2026-11-12): available + used funds from
        Kite across segments. Powers the dashboard "available cash"
        tile. Cached 5 min in-service (margins don't change
        sub-minute); FE can poll freely.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            margins = self.InvestmentService.fetchKiteMargins(user_id)
            return jsonify({"margins": margins}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite margins: {str(e)}")
            return jsonify({"error": "Failed to fetch margins"}), 500

    @Logger.standardLogger
    def fetchKiteTrades(self):
        """ak-zai2 Bundle 2 (2026-11-12): today's executed trades
        from Kite. Read-only; serves the FE "today's trades" surface
        + powers the daily ReconcileTradesTask cross-check.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            trades = self.InvestmentService.fetchKiteTrades(user_id)
            return jsonify({"trades": trades}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite trades: {str(e)}")
            return jsonify({"error": "Failed to fetch trades"}), 500

    # ───── ak-h1mq (2026-11-22): Kite MF page endpoints ─────
    #
    # Three thin controller wrappers over KiteService.get_mf_*
    # (methods existed; endpoints didn't until this bead). Backs
    # the Category B #10/#11/#12 FE Mutual Funds detail page.
    # READ-ONLY — no destructive / mutation paths. §23 CONSUME:
    # Kite's native shapes flow through unchanged for the FE.
    # KiteTokenExpired / KiteAuthError surface as reconnect-
    # required so the FE can prompt the user to re-auth without
    # a stale cache getting in the way.

    @Logger.standardLogger
    def fetchKiteMFHoldings(self):
        """ak-h1mq: GET /kite/mf/holdings — current MF holdings
        from Kite (Coin). One row per scheme with folio / units /
        avg buy price / last NAV / PnL.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            holdings = self.InvestmentService.fetchKiteMFHoldings(user_id)
            return jsonify({"holdings": holdings}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite MF holdings: {str(e)}")
            return jsonify({"error": "Failed to fetch MF holdings"}), 500

    @Logger.standardLogger
    def fetchKiteMFOrders(self):
        """ak-h1mq: GET /kite/mf/orders — MF buy/sell orders (last
        7 days per Kite's mf_orders() window). Powers the FE
        'Orders history' tab.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            orders = self.InvestmentService.fetchKiteMFOrders(user_id)
            return jsonify({"orders": orders}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite MF orders: {str(e)}")
            return jsonify({"error": "Failed to fetch MF orders"}), 500

    @Logger.standardLogger
    def fetchKiteMFSips(self):
        """ak-h1mq: GET /kite/mf/sips — active + completed MF SIPs
        from Kite. READ-ONLY per bead; SIP pause/edit/cancel
        actions are explicitly deferred to a separate SIP-
        management bead.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            sips = self.InvestmentService.fetchKiteMFSips(user_id)
            return jsonify({"sips": sips}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite MF SIPs: {str(e)}")
            return jsonify({"error": "Failed to fetch MF SIPs"}), 500

    @Logger.standardLogger
    def fetchKiteOrders(self):
        """ak-zi54 (2026-11-22): today's full order book from Kite.

        Returns every order placed today across all statuses
        (OPEN / COMPLETE / CANCELLED / REJECTED / TRIGGER_PENDING)
        — differs from /kite/trades which only returns FILLED
        orders. Powers the FE 'Today's Orders' complement to the
        existing 'Today's Trades' surface.

        READ-ONLY per bead — placing / modifying / cancelling
        orders is deferred to a separate bead. §23 #2 CONSUME:
        Kite's native order shape flows through unchanged.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            orders = self.InvestmentService.fetchKiteOrders(user_id)
            return jsonify({"orders": orders}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite orders: {str(e)}")
            return jsonify({"error": "Failed to fetch orders"}), 500

    # ───── ak-l8a7 (2026-11-22): per-order drill-down endpoints ─────
    #
    # Two thin READ-ONLY handlers wrapping KiteService.get_order_
    # history + get_order_trades. Backs the Category B #9 FE
    # drill-down modal from the Pending Orders surface — clicking
    # an order row opens the modal with status-transition timeline
    # + per-fill breakdown panels. Order_id is a Kite-issued
    # identifier (string) supplied as a path parameter; same
    # defensive validation as ak-dx98's alert_uuid handler:
    # refuse empty / obviously-malformed input before hitting
    # Kite so a missing param can't surface as a confusing
    # upstream error. §23 #2 CONSUME: Kite's native shapes flow
    # through the FE unchanged.

    @Logger.standardLogger
    def fetchKiteOrderHistory(self, order_id):
        """ak-l8a7: GET /kite/orders/<order_id>/history — status-
        transition timeline for a single order.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        if not order_id or not isinstance(order_id, str):
            return jsonify({"error": "order_id is required"}), 400

        try:
            history = self.InvestmentService.fetchKiteOrderHistory(
                user_id, order_id,
            )
            return jsonify({"history": history}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(
                f"Error fetching Kite order history "
                f"order_id={order_id}: {str(e)}"
            )
            return jsonify({"error": "Failed to fetch order history"}), 500

    @Logger.standardLogger
    def fetchKiteOrderTrades(self, order_id):
        """ak-l8a7: GET /kite/orders/<order_id>/trades — per-fill
        trade breakdown for a single order.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        if not order_id or not isinstance(order_id, str):
            return jsonify({"error": "order_id is required"}), 400

        try:
            trades = self.InvestmentService.fetchKiteOrderTrades(
                user_id, order_id,
            )
            return jsonify({"trades": trades}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(
                f"Error fetching Kite order trades "
                f"order_id={order_id}: {str(e)}"
            )
            return jsonify({"error": "Failed to fetch order trades"}), 500

    # ───── ak-dx98 Kite Bundle 3 (2026-11-12): monitoring surfaces ─────

    @Logger.standardLogger
    def fetchKiteGtts(self):
        """ak-dx98 Bundle 3: active GTT monitors with LTP-distance
        enrichment. Serves the FE "Active monitors" dashboard
        section. 5-min per-user cache in-service (GTTs don't change
        sub-minute at the account level). KiteAuthError → the
        shared reconnect response shape.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            gtts = self.InvestmentService.fetchKiteGtts(user_id)
            return jsonify({"gtts": gtts}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite GTTs: {str(e)}")
            return jsonify({"error": "Failed to fetch GTTs"}), 500

    @Logger.standardLogger
    def fetchKiteAlerts(self):
        """ak-dx98 Bundle 3: price alerts for the user. Serves the
        FE "Active monitors" alerts panel. 5-min per-user cache
        in-service. Alert-history drill-down uses the per-uuid
        endpoint below.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            alerts = self.InvestmentService.fetchKiteAlerts(user_id)
            return jsonify({"alerts": alerts}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error fetching Kite alerts: {str(e)}")
            return jsonify({"error": "Failed to fetch alerts"}), 500

    @Logger.standardLogger
    def fetchKiteAlertHistory(self, alert_uuid):
        """ak-dx98 Bundle 3: trigger log for a single alert.

        ``alert_uuid`` is the Kite-issued alert identifier (path
        parameter — Flask already URL-decodes). We refuse empty /
        obviously-malformed input before touching Kite so a missing
        param can't produce a confusing upstream error.

        No cache — drill-down view wants fresh data.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        if not alert_uuid or not isinstance(alert_uuid, str):
            return jsonify({"error": "Alert UUID is required"}), 400

        try:
            history = self.InvestmentService.fetchKiteAlertHistory(
                user_id, alert_uuid,
            )
            return jsonify({"history": history}), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(
                f"Error fetching Kite alert history "
                f"uuid={alert_uuid}: {str(e)}"
            )
            return jsonify({"error": "Failed to fetch alert history"}), 500

    @Logger.standardLogger
    def syncKiteHoldings(self):
        """Sync Kite holdings to local database.

        ak-n9im (2026-11-12) — **upsert, non-destructive** (see
        ``StocksService.sync_kite_holdings_to_db`` for the full
        semantic contract). Rows for symbols Kite no longer
        reports are PRESERVED (purchase-history ledger kept).
        All writes are in a single transaction rolled back on
        failure, so a mid-sync Kite failure cannot leave the DB
        half-synced. Safe to expose as a user-initiated refresh
        button without a FE confirm-gate (verified ak-gkqp FE-4).
        """
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            result = self.InvestmentService.syncKiteHoldings(user_id)
            return jsonify({
                "message": "Holdings synced successfully",
                "result": result
            }), 200
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as e:
            self.logger.error(f"Error syncing Kite holdings: {str(e)}")
            return jsonify({"error": "Failed to sync holdings"}), 500

    @Logger.standardLogger
    def fetchKiteTokenStatus(self):
        """Report whether the Kite session is live, and how to reconnect."""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            return jsonify(self.InvestmentService.getKiteTokenStatus(user_id)), 200
        except Exception as e:
            self.logger.error(f"Error fetching Kite token status: {str(e)}")
            return jsonify({"error": "Failed to fetch Kite status"}), 500

    @Logger.standardLogger
    def getKiteLoginUrl(self):
        """Get Kite Connect login URL for frontend"""
        try:
            login_url = self.InvestmentService.getKiteLoginUrl()
            return jsonify({"login_url": login_url}), 200
        except Exception as e:
            self.logger.error(f"Error getting Kite login URL: {str(e)}")
            return jsonify({"error": "Failed to get login URL"}), 500

    # ak-v55t: Kite instrument-master autocomplete search.
    #
    # Public market data (per bead §4 PII pass — N/A); still goes
    # through the before_request auth middleware so an unauth
    # client can't enumerate the master. Lightweight filter:
    # LIKE on tradingsymbol + name, optional exchange chip, limit
    # clamped to [1, 100]. Serves from the daily-refreshed
    # kite_instruments cache — NO Kite broker call per request.
    @Logger.standardLogger
    def searchKiteInstruments(self):
        """GET /kite/instruments/search?q=<query>&exchange=<EX>&limit=<N>.

        Response: ``{"results": [...], "count": N, "query": ...}``.
        Empty / whitespace query → 400 (prevent full-master
        enumeration). Unknown exchange returns an empty result
        list rather than 400 — matches the behaviour of an unknown
        search term.
        """
        q = (request.args.get("q") or "").strip()
        if not q:
            return jsonify({
                "error": (
                    "'q' query param is required and must be "
                    "non-empty — bare search would return the "
                    "full instrument universe"
                ),
            }), 400
        exchange = (request.args.get("exchange") or "").strip() or None
        limit_raw = request.args.get("limit", "20")
        try:
            limit = int(limit_raw)
        except (TypeError, ValueError):
            return jsonify({
                "error": f"'limit' must be an integer; got {limit_raw!r}",
            }), 400

        try:
            results = self.InvestmentService.search_kite_instruments(
                q, exchange=exchange, limit=limit,
            )
        except Exception as exc:
            self.logger.error(
                f"Instrument search failed q={q!r} exchange={exchange!r}:"
                f" {exc}",
            )
            return jsonify({"error": "Instrument search failed"}), 500

        return jsonify({
            "results": results,
            "count": len(results),
            "query": q,
            "exchange": exchange,
        }), 200

    @Logger.standardLogger
    def generateKiteSession(self):
        """Generate Kite session using request token from frontend"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        data = request.get_json()
        if not data or 'request_token' not in data:
            return jsonify({"error": "Request token is required"}), 400

        request_token = data['request_token']
        
        try:
            session_data = self.InvestmentService.generateKiteSession(user_id, request_token)
            return jsonify({
                "message": "Session generated successfully",
                "session_data": session_data
            }), 200
        except Exception as e:
            self.logger.error(f"Error generating Kite session: {str(e)}")
            return jsonify({"error": "Failed to generate session"}), 500
