import requests
import json
import os
import fcntl
import time
from datetime import datetime, timedelta
from decimal import Decimal
from utils.logger import Logger
from services.money_utils import money, q2, q4


# ak-lvu A.6 — vintage date of the last hardcoded rate refresh. Used for
# the read-side "stale" indicator when live rates are unreachable but
# we're still willing to serve a read-only display. Never used for
# writes (writes fail-closed on API unavailable per bead A.6).
_FALLBACK_VINTAGE = "2024-Q4"


class CurrencyUnavailableError(RuntimeError):
    """ak-lvu A.6 — raised when the write path calls
    `convert_to_inr_with_source(...)` and the live rate API is
    unreachable AND the caller has opted out of the 2024-vintage
    fallback (writes always opt out by default).

    The invoiceService write path catches this and surfaces
    "FX rate unavailable, retry later" to the user + LLM — no silent
    fallback that would enshrine a 2024 rate as if it were today's."""


class CurrencyService:
    """
    Service to fetch and cache live currency exchange rates from ExchangeRate-API.
    Optimized for INR-centric operations with robust restart-safe caching.

    ak-lvu A.6: WRITES fail-closed on API unavailable.
    - `convert_to_inr(amount, currency)` returns just the float INR value
      for backwards compatibility (legacy read-side callers).
    - `convert_to_inr_with_source(amount, currency)` is the new
      write-side entry point: returns a dict
        {"inr_amount": Decimal, "fx_rate": Decimal, "fx_rate_source": str,
         "converted_at": datetime}
      or RAISES `CurrencyUnavailableError` if the live API is down AND
      the caller opts out of the vintage-2024 fallback (default: writes
      DO opt out).
    """

    def __init__(self):
        self.logger = Logger(__name__).get_logger()

        # Get API key from environment variable
        self.api_key = os.getenv('EXCHANGE_RATE_API_KEY')
        if not self.api_key:
            self.logger.error("EXCHANGE_RATE_API_KEY not found in environment variables. Please set your API key.")
            raise ValueError("ExchangeRate-API key is required. Please set EXCHANGE_RATE_API_KEY environment variable.")

        # Use v6 authenticated endpoint - Always fetch USD rates (most reliable and comprehensive)
        self.base_url = f"https://v6.exchangerate-api.com/v6/{self.api_key}/latest"
        self.primary_base_currency = "USD"  # Always use USD as base for consistency

        # Robust cache setup
        self.cache_dir = os.path.join(os.getcwd(), 'services', 'assets', 'cache')
        self.cache_file = os.path.join(self.cache_dir, 'exchange_rates_cache.json')
        self.cache_duration_hours = 12  # Shorter cache for more current rates

        # Ensure cache directory exists with proper permissions
        os.makedirs(self.cache_dir, mode=0o755, exist_ok=True)

        # ak-lvu A.6 — legacy fallback rates retained ONLY for read-side
        # display when explicit `allow_fallback=True` is passed
        # (backwards compat with dashboard read paths). Writes never
        # touch this map. Vintage tag surfaces to the caller so UI can
        # render a "stale rates: 2024-Q4" indicator.
        self.fallback_rates_to_inr = {
            'USD': 83.25,   # 1 USD = 83.25 INR
            'GBP': 105.50,  # 1 GBP = 105.50 INR
            'EUR': 90.75,   # 1 EUR = 90.75 INR
            'JPY': 0.56,    # 1 JPY = 0.56 INR
            'CAD': 61.50,   # 1 CAD = 61.50 INR
            'AUD': 54.25,   # 1 AUD = 54.25 INR
            'INR': 1.0      # 1 INR = 1 INR
        }

        # Initialize cache on startup
        self._initialize_cache()

    def _initialize_cache(self):
        """Initialize cache system and validate existing cache"""
        try:
            # Create cache file if it doesn't exist
            if not os.path.exists(self.cache_file):
                self.logger.info("Creating new exchange rates cache file")
                self._write_empty_cache()
            else:
                # Validate existing cache
                self._validate_cache_integrity()
                
        except Exception as e:
            self.logger.error(f"Error initializing cache: {str(e)}")
            self._write_empty_cache()

    def _write_empty_cache(self):
        """Write an empty cache structure"""
        empty_cache = {
            'created_at': datetime.now().isoformat(),
            'last_api_call': None,
            'rates': {}
        }
        self._write_cache_safely(empty_cache)

    def _validate_cache_integrity(self):
        """Validate that cache file has proper structure"""
        try:
            cache = self._read_cache_safely()
            if not isinstance(cache, dict) or 'rates' not in cache:
                self.logger.warning("Cache structure invalid, reinitializing")
                self._write_empty_cache()
        except Exception:
            self.logger.warning("Cache file corrupted, reinitializing")
            self._write_empty_cache()

    def _read_cache_safely(self):
        """Thread-safe cache reading with file locking"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                with open(self.cache_file, 'r') as f:
                    # Use file locking to prevent read during write
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    try:
                        cache = json.load(f)
                        return cache
                    finally:
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except (IOError, json.JSONDecodeError) as e:
                if attempt < max_attempts - 1:
                    time.sleep(0.1)  # Brief wait before retry
                    continue
                raise e
        return {}

    def _write_cache_safely(self, cache_data):
        """Thread-safe cache writing with atomic operations"""
        temp_file = f"{self.cache_file}.tmp"
        try:
            # Write to temporary file first
            with open(temp_file, 'w') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(cache_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            
            # Atomic move to replace cache file
            os.rename(temp_file, self.cache_file)
            
        except Exception as e:
            # Clean up temp file if it exists
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise e

    def convert_to_inr(self, amount, from_currency):
        """
        Convert amount from any currency to INR (LEGACY read-side entry).

        Kept for backwards compat with dashboard read paths that only
        need a display value. Silently falls back to 2024-vintage rates
        on API failure — DO NOT USE for writes. Writes must call
        `convert_to_inr_with_source(...)` which fails closed.
        """
        if from_currency.upper() == 'INR':
            return float(amount)

        try:
            # Get USD-based rates (most comprehensive and reliable)
            usd_rates = self._get_usd_rates()

            if from_currency.upper() == 'USD':
                # Direct USD to INR conversion
                inr_rate = usd_rates['INR']
                converted = float(amount) * inr_rate
            else:
                # Convert via USD: amount -> USD -> INR
                from_rate = usd_rates.get(from_currency.upper())
                if not from_rate:
                    raise ValueError(f"Currency {from_currency} not supported")

                inr_rate = usd_rates['INR']
                # Convert to USD first, then to INR
                usd_amount = float(amount) / from_rate
                converted = usd_amount * inr_rate

            self.logger.info(f"Converted {amount} {from_currency} = {converted:.2f} INR")
            return round(converted, 2)

        except Exception as e:
            self.logger.error(f"Live conversion failed for {amount} {from_currency}: {str(e)}")
            return self._fallback_convert_to_inr(amount, from_currency)

    def convert_to_inr_with_source(
        self,
        amount,
        from_currency,
        *,
        allow_fallback: bool = False,
    ) -> dict:
        """ak-lvu A.6: write-side FX conversion returning full audit metadata.

        Returns:
            {
                "inr_amount":    Decimal (2dp),
                "fx_rate":       Decimal (4dp) — INR per 1 unit of from_currency,
                "fx_rate_source": str — one of
                                  "exchangerate-api-v6:live"       (fresh API hit),
                                  "exchangerate-api-v6:cached:<hh>h" (cache within TTL),
                                  "fallback:<vintage>"             (opt-in only),
                                  "identity"                       (INR → INR),
                "converted_at":  datetime (UTC-naive, matches DB DateTime shape),
            }

        Raises:
            CurrencyUnavailableError — live API AND cache both
              unavailable AND allow_fallback=False (default for writes).
            ValueError — unsupported currency.

        The invoiceService write path calls this WITHOUT allow_fallback
        so a genuine outage blocks the write with a clear signal rather
        than silently enshrining a 2024 rate as "today's". Read-side
        paths that want a best-effort value can pass allow_fallback=True.
        """
        currency_str = str(from_currency).upper()
        amount_d = money(amount)
        now = datetime.utcnow()

        # INR → INR: identity conversion, no API call needed. fx_rate=1,
        # source tagged so audits distinguish "conversion happened" from
        # "no conversion needed".
        if currency_str == 'INR':
            return {
                "inr_amount": q2(amount_d),
                "fx_rate": q4(Decimal("1")),
                "fx_rate_source": "identity",
                "converted_at": now,
            }

        # Try live rates (cache-first).
        rate_meta = None
        try:
            usd_rates, cache_age_hours = self._get_usd_rates_with_age()
            if currency_str == 'USD':
                inr_per_from = money(usd_rates['INR'])
            else:
                from_rate = usd_rates.get(currency_str)
                if not from_rate:
                    raise ValueError(f"Currency {currency_str} not supported")
                # amount_from × (inr / from) = amount_from × (usd_rate['INR'] / from_rate)
                inr_per_from = money(usd_rates['INR']) / money(from_rate)

            source_tag = (
                "exchangerate-api-v6:live"
                if cache_age_hours == 0
                else f"exchangerate-api-v6:cached:{cache_age_hours:.1f}h"
            )
            rate_meta = {
                "fx_rate": q4(inr_per_from),
                "fx_rate_source": source_tag,
            }
        except Exception as exc:
            self.logger.warning(
                f"ak-lvu A.6: live FX unavailable for {currency_str}: {exc}"
            )

        # If live path failed and caller allowed the vintage fallback,
        # use it (with the source tagged so any downstream audit sees the
        # provenance). Otherwise fail closed.
        if rate_meta is None:
            if not allow_fallback:
                raise CurrencyUnavailableError(
                    f"FX rate unavailable for {currency_str}. Retry when "
                    f"exchange-rate service is reachable."
                )
            fallback_rate = self.fallback_rates_to_inr.get(currency_str)
            if fallback_rate is None:
                raise ValueError(f"Currency {currency_str} not supported")
            rate_meta = {
                "fx_rate": q4(money(fallback_rate)),
                "fx_rate_source": f"fallback:{_FALLBACK_VINTAGE}",
            }

        inr_amount = q2(amount_d * rate_meta["fx_rate"])
        return {
            "inr_amount": inr_amount,
            "fx_rate": rate_meta["fx_rate"],
            "fx_rate_source": rate_meta["fx_rate_source"],
            "converted_at": now,
        }

    def _get_usd_rates_with_age(self) -> tuple:
        """Return (rates_dict, age_hours). age_hours == 0 means the rates
        were just fetched from the API in this call; > 0 means served
        from cache within TTL."""
        cached_data = self._get_cached_usd_rates()
        if cached_data:
            self.logger.debug("Using cached USD exchange rates")
            cached_time = datetime.fromisoformat(cached_data['timestamp'])
            age_hours = (datetime.now() - cached_time).total_seconds() / 3600
            return cached_data['rates'], age_hours
        self.logger.info("Fetching fresh USD exchange rates from ExchangeRate-API")
        rates_data = self._fetch_fresh_usd_rates()
        self._cache_usd_rates(rates_data)
        return rates_data['rates'], 0.0

    def _get_usd_rates(self):
        """Get USD-based exchange rates (cached or fresh from API)"""
        try:
            # Check cache first
            cached_data = self._get_cached_usd_rates()
            if cached_data:
                self.logger.debug("Using cached USD exchange rates")
                return cached_data['rates']
            
            # Fetch fresh rates from API
            self.logger.info("Fetching fresh USD exchange rates from ExchangeRate-API")
            rates_data = self._fetch_fresh_usd_rates()
            
            # Cache the fresh rates
            self._cache_usd_rates(rates_data)
            
            return rates_data['rates']
            
        except Exception as e:
            self.logger.error(f"Failed to get USD rates: {str(e)}")
            raise

    def _get_cached_usd_rates(self):
        """Get cached USD rates if still valid"""
        try:
            cache = self._read_cache_safely()
            usd_data = cache['rates'].get('USD')
            
            if not usd_data:
                return None
            
            # Check if cache is still valid
            cached_time = datetime.fromisoformat(usd_data['timestamp'])
            if datetime.now() - cached_time < timedelta(hours=self.cache_duration_hours):
                return usd_data
            else:
                self.logger.debug("USD rates cache expired")
                return None
                
        except Exception as e:
            self.logger.error(f"Error reading USD cache: {str(e)}")
            return None

    def _fetch_fresh_usd_rates(self):
        """Fetch fresh USD exchange rates from API"""
        url = f"{self.base_url}/{self.primary_base_currency}"
        
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle API error responses (v6 format)
        if 'result' in data and data['result'] == 'error':
            error_type = data.get('error-type', 'unknown')
            raise ValueError(f"ExchangeRate-API error: {error_type}")
        
        # Validate response structure (v6 uses 'conversion_rates')
        if 'conversion_rates' not in data:
            raise ValueError(f"Invalid response format from ExchangeRate-API v6")
        
        rates_data = {
            'base': data.get('base_code', self.primary_base_currency),
            'rates': data['conversion_rates'],
            'timestamp': datetime.now().isoformat(),
            'last_updated': data.get('time_last_update_utc', datetime.now().strftime('%Y-%m-%d')),
            'next_update': data.get('time_next_update_utc'),
            'api_result': data.get('result', 'success')
        }
        
        self.logger.info(f"Successfully fetched {len(rates_data['rates'])} exchange rates")
        return rates_data

    def _cache_usd_rates(self, rates_data):
        """Cache USD rates data safely"""
        try:
            cache = self._read_cache_safely()
            
            # Update cache with new USD rates
            cache['rates']['USD'] = rates_data
            cache['last_api_call'] = datetime.now().isoformat()
            
            # Write updated cache
            self._write_cache_safely(cache)
            
            self.logger.info("Cached fresh USD exchange rates")
            
        except Exception as e:
            self.logger.error(f"Failed to cache USD rates: {str(e)}")
            # Don't raise - caching failure shouldn't break the conversion

    def _fallback_convert_to_inr(self, amount, from_currency):
        """Fallback conversion using hardcoded rates"""
        rate = self.fallback_rates_to_inr.get(from_currency.upper(), 1.0)
        converted = float(amount) * rate
        
        self.logger.warning(f"Using fallback rate: {amount} {from_currency} = {converted:.2f} INR (rate: {rate})")
        return round(converted, 2)

    def get_cache_info(self):
        """Get information about current cache status"""
        try:
            if not os.path.exists(self.cache_file):
                return {"status": "No cache file"}
            
            cache = self._read_cache_safely()
            info = {}
            
            if 'rates' in cache and 'USD' in cache['rates']:
                usd_data = cache['rates']['USD']
                cached_time = datetime.fromisoformat(usd_data['timestamp'])
                age_hours = (datetime.now() - cached_time).total_seconds() / 3600
                info['USD'] = {
                    'last_updated': usd_data['last_updated'],
                    'cached_at': usd_data['timestamp'],
                    'age_hours': round(age_hours, 2),
                    'valid': age_hours < self.cache_duration_hours,
                    'api_result': usd_data.get('api_result', 'unknown')
                }
            
            info['cache_created'] = cache.get('created_at')
            info['last_api_call'] = cache.get('last_api_call')
            
            return info
            
        except Exception as e:
            return {"error": str(e)}

    def get_api_status(self):
        """Get API configuration and status information"""
        return {
            'api_key_configured': bool(self.api_key and len(self.api_key) > 10),
            'api_endpoint': 'ExchangeRate-API v6',
            'endpoint_url': self.base_url,
            'cache_duration_hours': self.cache_duration_hours,
            'cache_file_exists': os.path.exists(self.cache_file),
            'primary_currency': self.primary_base_currency,
            'supported_currencies': list(self.fallback_rates_to_inr.keys()),
            'cache_info': self.get_cache_info()
        }

    def force_refresh_rates(self):
        """Force refresh of exchange rates (bypass cache)"""
        try:
            self.logger.info("Force refreshing exchange rates...")
            rates_data = self._fetch_fresh_usd_rates()
            self._cache_usd_rates(rates_data)
            return rates_data
        except Exception as e:
            self.logger.error(f"Failed to force refresh rates: {str(e)}")
            raise