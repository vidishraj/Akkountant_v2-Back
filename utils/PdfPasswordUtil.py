"""
Utility to generate common Indian bank PDF password patterns from user personal info.

Indian banks typically protect statement PDFs with passwords derived from:
- Date of birth (DDMMYYYY, DD-MM-YYYY, DDMM, MMDDYYYY, etc.)
- PAN number (uppercase, lowercase)
- Phone number (full, last 4 digits)
- First name + DOB combos (JOHN25091990, john25091990)
- Customer ID (HDFC etc.)
- UAN number (EPF passbooks)

The email body usually contains a hint like "Password is your date of birth in DDMMYYYY format".
We parse that hint to prioritize the right candidates first.
"""

import re
import fitz  # PyMuPDF
from utils.logger import Logger

logger = Logger(__name__).get_logger()

# ── Hint keyword → candidate group mapping ──────────────────────
# Each entry: (keywords_to_look_for, group_tag)
_HINT_PATTERNS = [
    # DOB patterns
    (r'date\s*of\s*birth|dob|birth\s*date|d\.o\.b', 'dob'),
    # PAN patterns
    (r'\bpan\b|permanent\s*account\s*number', 'pan'),
    # Phone/mobile patterns
    (r'mobile|phone|registered\s*(mobile|number)|contact\s*number', 'phone'),
    # Customer ID patterns
    (r'customer\s*id|cust\.?\s*id|cif\b|crn\b', 'customer_id'),
    # UAN patterns
    (r'\buan\b|universal\s*account', 'uan'),
    # Name + DOB combo patterns
    (r'(first|name).*(\+|and|&).*dob|(name|first).*birth', 'name_dob'),
    # Name-only patterns
    (r'first\s*name|account\s*holder\s*name|your\s*name', 'name'),
]


def extract_password_hint(email_body):
    """
    Extract password hint from an email body.
    Returns a list of detected hint group tags (e.g. ['dob'], ['name_dob', 'dob']).
    Empty list if no hint found.
    """
    if not email_body:
        return []

    body_lower = email_body.lower()
    # Only look at password-related context — narrow the search window
    # Look for sentences/phrases near "password" or "protected" or "open"
    password_context = ""
    for keyword in ["password", "protected", "open the", "open this", "unlock", "encrypted"]:
        idx = body_lower.find(keyword)
        if idx >= 0:
            # Grab a window around the keyword (200 chars before and after)
            start = max(0, idx - 200)
            end = min(len(body_lower), idx + 300)
            password_context += body_lower[start:end] + " "

    # If no password-related context found, scan the full body (some emails are short)
    search_text = password_context if password_context else body_lower

    detected = []
    for pattern, tag in _HINT_PATTERNS:
        if re.search(pattern, search_text, re.IGNORECASE):
            detected.append(tag)

    return detected


def generate_password_candidates(personal_info, hint_tags=None):
    """
    Generate a list of common password candidates from user personal info.
    Returns a list of strings to try, ordered by likelihood.

    If hint_tags is provided (from extract_password_hint), candidates matching
    the hint are placed first for faster unlocking.

    personal_info is a dict or model object with fields:
    first_name, last_name, date_of_birth (DD/MM/YYYY), pan_number,
    phone_number, uan_number, customer_id_hdfc
    """
    # Extract fields (handle both dict and object)
    if hasattr(personal_info, 'first_name'):
        first_name = personal_info.first_name or ""
        last_name = personal_info.last_name or ""
        dob_raw = personal_info.date_of_birth
        dob = dob_raw.strftime("%d/%m/%Y") if hasattr(dob_raw, 'strftime') else (dob_raw or "")
        pan = personal_info.pan_number or ""
        phone = personal_info.phone_number or ""
        uan = personal_info.uan_number or ""
        cust_id_hdfc = personal_info.customer_id_hdfc or ""
        phone2 = getattr(personal_info, 'phone_number_2', "") or ""
    else:
        first_name = personal_info.get("first_name", "") or ""
        last_name = personal_info.get("last_name", "") or ""
        dob = personal_info.get("date_of_birth", "") or ""
        pan = personal_info.get("pan_number", "") or ""
        phone = personal_info.get("phone_number", "") or ""
        phone2 = personal_info.get("phone_number_2", "") or ""
        uan = personal_info.get("uan_number", "") or ""
        cust_id_hdfc = personal_info.get("customer_id_hdfc", "") or ""

    # Parse DOB parts
    dob_parts = _parse_dob(dob)
    dd = dob_parts.get("dd", "")
    mm = dob_parts.get("mm", "")
    yyyy = dob_parts.get("yyyy", "")
    yy = yyyy[-2:] if len(yyyy) == 4 else ""

    # Build candidates in groups so we can prioritize by hint
    groups = {
        "dob": [],
        "pan": [],
        "phone": [],
        "name_dob": [],
        "name": [],
        "customer_id": [],
        "uan": [],
        "name_pan": [],
    }

    # ── DOB-based ────────────────────────────────────────────────
    if dd and mm and yyyy:
        groups["dob"].extend([
            f"{dd}{mm}{yyyy}",       # DDMMYYYY — most common
            f"{dd}-{mm}-{yyyy}",     # DD-MM-YYYY
            f"{dd}/{mm}/{yyyy}",     # DD/MM/YYYY
            f"{dd}{mm}{yy}",         # DDMMYY
            f"{mm}{dd}{yyyy}",       # MMDDYYYY
            f"{yyyy}{mm}{dd}",       # YYYYMMDD (ISO)
            f"{dd}{mm}",             # DDMM
        ])

    # ── PAN-based ────────────────────────────────────────────────
    if pan:
        groups["pan"].extend([pan.upper(), pan.lower()])

    # ── Phone-based ──────────────────────────────────────────────
    for ph in [phone, phone2]:
        if ph:
            clean_phone = ph.replace(" ", "").replace("-", "").replace("+91", "")
            if len(clean_phone) >= 10:
                groups["phone"].extend([
                    clean_phone[-10:],   # Last 10 digits
                    clean_phone[-4:],    # Last 4 digits
                    clean_phone[-6:],    # Last 6 digits
                ])

    # ── Name + DOB combos ────────────────────────────────────────
    if first_name and dd and mm:
        fn_upper = first_name.strip().upper()
        fn_lower = first_name.strip().lower()
        fn_cap = first_name.strip().capitalize()
        for fn in [fn_upper, fn_lower, fn_cap]:
            # name[:4] + DDMM (very common — ICICI, YES Bank, etc.)
            groups["name_dob"].extend([
                f"{fn[:4]}{dd}{mm}",
                f"{fn}{dd}{mm}",
            ])
            if yyyy:
                # name[:4] + DDMMYYYY (full DOB)
                groups["name_dob"].extend([
                    f"{fn[:4]}{dd}{mm}{yyyy}",
                    f"{fn}{dd}{mm}{yyyy}",
                    # name[:4] + DDMMYY (2-digit year)
                    f"{fn[:4]}{dd}{mm}{yy}",
                ])

    # ── Name-only ────────────────────────────────────────────────
    if first_name:
        groups["name"].extend([first_name.strip().lower(), first_name.strip().upper()])

    # ── HDFC Customer ID ─────────────────────────────────────────
    if cust_id_hdfc:
        groups["customer_id"].append(cust_id_hdfc)

    # ── UAN-based ────────────────────────────────────────────────
    if uan:
        groups["uan"].append(uan)

    # ── Name + PAN combos ────────────────────────────────────────
    if first_name and pan:
        groups["name_pan"].extend([
            f"{first_name.strip().lower()}{pan.upper()}",
            f"{pan.upper()}{first_name.strip().lower()}",
        ])

    # ── Assemble final list: hinted groups first, then the rest ──
    # Default priority order (most common bank password patterns first)
    default_order = ["dob", "pan", "phone", "name_dob", "name", "customer_id", "uan", "name_pan"]

    if hint_tags:
        # Put hinted groups at the front, then remaining groups in default order
        priority = list(hint_tags)
        for g in default_order:
            if g not in priority:
                priority.append(g)
        logger.info(f"Password hint detected: {hint_tags} — prioritized order: {priority[:3]}...")
    else:
        priority = default_order

    candidates = []
    for group_name in priority:
        candidates.extend(groups.get(group_name, []))

    # ── Dedup while preserving order ──────────────────────────────
    seen = set()
    unique = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            unique.append(c)

    return unique


def try_unlock_pdf(pdf_path, password_candidates):
    """
    Try to unlock a PDF with a list of password candidates.
    Returns the working password or None if none work.
    Uses PyMuPDF for fast in-process attempts (no API calls).
    """
    try:
        doc = fitz.open(pdf_path)
        if not doc.needs_pass:
            doc.close()
            return None  # Not password-protected

        for pwd in password_candidates:
            if doc.authenticate(pwd):
                doc.close()
                logger.info(f"PDF unlocked with password pattern (length={len(pwd)})")
                return pwd

        doc.close()
        logger.warning(f"None of {len(password_candidates)} password candidates worked for {pdf_path}")
        return None

    except Exception as e:
        logger.error(f"Error trying to unlock PDF: {e}")
        return None


def _parse_dob(dob_str):
    """Parse DOB string in DD/MM/YYYY or DD-MM-YYYY or DDMMYYYY format."""
    if not dob_str:
        return {}

    dob_str = dob_str.strip()

    # DD/MM/YYYY or DD-MM-YYYY
    for sep in ["/", "-"]:
        if sep in dob_str:
            parts = dob_str.split(sep)
            if len(parts) == 3:
                return {"dd": parts[0].zfill(2), "mm": parts[1].zfill(2), "yyyy": parts[2]}

    # DDMMYYYY (8 digits)
    if len(dob_str) == 8 and dob_str.isdigit():
        return {"dd": dob_str[:2], "mm": dob_str[2:4], "yyyy": dob_str[4:]}

    return {}
