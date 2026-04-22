"""
Nigeria E-Invoicing Dashboard - Sage X3 Edition
================================================
- Reads invoices from Sage X3 via REST API
- Posts to FIRS via Flick Network API
- PDF generation with QR codes
- Same workflow as Sage 50 version

USAGE:
    1. Configure config_x3.py with X3 credentials
    2. Run: python app_x3.py
    3. Open: http://localhost:5001
"""

import os
import io
import json
import sqlite3
import threading
import requests
from datetime import datetime, date, timedelta
from decimal import Decimal
from functools import wraps
from flask import Flask, render_template, jsonify, send_file, request, session, redirect, url_for

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from sage_x3_reader import SageX3Reader, to_float, to_str
from config_x3 import (
    X3_BASE_URL, X3_FOLDER, X3_USERNAME, X3_PASSWORD,
    API_BASE_URL, API_KEY, PARTICIPANT_ID
)

# ============================================================
# CONFIGURATION
# ============================================================
# X3 Connection loaded from config_x3.py

# FIRS environment switch: "test" or "prod"
FIRS_ENV = os.environ.get("FIRS_ENV", "test").strip().lower()

# Shared FIRS endpoints (same for all companies)
FIRS_TEST_URL = os.environ.get("FIRS_TEST_URL", "https://preprod-api.cryptwaresystemsltd.com")
FIRS_PROD_URL = os.environ.get("FIRS_PROD_URL", "https://api.cryptwaresystemsltd.com")

# Shared supplier defaults (both companies are in Lagos, NG)
_DEFAULT_ADDRESS = {
    "street_name": "Lagos",
    "city_name": "Lagos",
    "postal_zone": "100001",
    "country": "NG",
}

# Fallback API config — only used if an invoice can't be resolved to a company
DEFAULT_API_URL = FIRS_TEST_URL if FIRS_ENV == "test" else FIRS_PROD_URL
DEFAULT_API_KEY = API_KEY  # legacy FIRS_API_KEY fallback

def _env(name, default=""):
    v = os.environ.get(name, default)
    return v if v is not None else default

LOGO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo")

# Per-company FIRS config. URL is shared; only keys and supplier identity vary.
COMPANY_API_CONFIG = {
    "SWI03": {
        "name": "Swift Oil",
        "logo": os.path.join(LOGO_DIR, "swiftoil.jpg"),
        "test": {"url": FIRS_TEST_URL, "key": _env("SWIFT_FIRS_TEST_KEY")},
        "prod": {"url": FIRS_PROD_URL, "key": _env("SWIFT_FIRS_PROD_KEY")},
        "supplier": {
            "name": _env("SWIFT_SUPPLIER_NAME", "Swift Oil Limited"),
            "tin": _env("SWIFT_SUPPLIER_TIN", ""),
            "email": _env("SWIFT_SUPPLIER_EMAIL", "info@swiftoil.com"),
            "telephone": _env("SWIFT_SUPPLIER_PHONE", "+234"),
            "business_description": "Oil and Gas Distribution",
            **_DEFAULT_ADDRESS,
        },
    },
    "CHO01": {
        "name": "Chorus Energy",
        "logo": os.path.join(LOGO_DIR, "chorusEnergy.png"),
        "test": {"url": FIRS_TEST_URL, "key": _env("CHORUS_FIRS_TEST_KEY")},
        "prod": {"url": FIRS_PROD_URL, "key": _env("CHORUS_FIRS_PROD_KEY")},
        "supplier": {
            "name": _env("CHORUS_SUPPLIER_NAME", "Chorus Energy Limited"),
            "tin": _env("CHORUS_SUPPLIER_TIN", ""),
            "email": _env("CHORUS_SUPPLIER_EMAIL", "info@chorusenergy.com"),
            "telephone": _env("CHORUS_SUPPLIER_PHONE", "+234"),
            "business_description": "Energy Distribution",
            **_DEFAULT_ADDRESS,
        },
    },
}

# Legacy name -> code map so rows that still only have company_name can be
# resolved to their company_code for credential lookup.
LEGACY_NAME_TO_CODE = {
    "SWIFT OIL  LEKKI": "SWI03",
    "SWIFT OIL LEKKI": "SWI03",
    "SWIFT LEKK": "SWI03",
    "CHORUS ENERGY  LEKKI": "CHO01",
    "CHORUS ENERGY LEKKI": "CHO01",
}

def resolve_company_code(inv_row):
    """Return the best-guess X3 SALFCY code for a DB invoice row."""
    code = (inv_row.get("company_code") or "").strip()
    if code:
        return code
    name = (inv_row.get("company_name") or "").strip()
    return LEGACY_NAME_TO_CODE.get(name, "")

def get_company_api(company_code):
    """Return (url, api_key, supplier, logo_path) for a company, honoring FIRS_ENV.

    Falls back to DEFAULT_API_URL/KEY if the company has no config.
    """
    cfg = COMPANY_API_CONFIG.get(company_code or "")
    if not cfg:
        return DEFAULT_API_URL, DEFAULT_API_KEY, _default_supplier(), None
    env_cfg = cfg.get(FIRS_ENV) or cfg.get("test") or {}
    url = env_cfg.get("url") or DEFAULT_API_URL
    key = env_cfg.get("key") or DEFAULT_API_KEY
    supplier = cfg.get("supplier") or _default_supplier()
    logo = cfg.get("logo")
    return url, key, supplier, logo

def _default_supplier():
    # Rarely used: only hit when an invoice can't be mapped to a known company.
    # Falls back to the first configured company's supplier profile.
    first = next(iter(COMPANY_API_CONFIG.values()), None)
    if first:
        return dict(first["supplier"])
    return {"name": "Unknown", "tin": "", "email": "", "telephone": "+234",
            "business_description": "", **_DEFAULT_ADDRESS}

# Legacy global — kept so any other caller that still references it doesn't
# break, but post_to_firs now uses get_company_api() instead.
API_URL = DEFAULT_API_URL
API_HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": DEFAULT_API_KEY,
}
SUPPLIER = _default_supplier()

# Tax categories
TAX_CAT_STANDARD = os.environ.get("TAX_CAT_STANDARD", "STANDARD_VAT")
TAX_CAT_EXEMPT = os.environ.get("TAX_CAT_EXEMPT", "ZERO_VAT")

# Only sync invoices from these X3 sales sites. Leave blank to sync everything.
# Override via env var: ALLOWED_COMPANY_CODES="SWI03,CHO01"
ALLOWED_COMPANY_CODES = [
    c.strip() for c in os.environ.get("ALLOWED_COMPANY_CODES", "SWI03,CHO01").split(",")
    if c.strip()
]

# Display-name -> code mapping for dashboard filter UI.
# "label" is what the user sees, "codes" is the set of X3 SALFCY codes it maps to,
# "names" are legacy company_name variants to also match (for rows synced before
# company_code existed, or X3 records where the same site has multiple display
# labels like "SWIFT LEKK" vs "SWIFT OIL LEKKI" — both are SWI03).
COMPANY_FILTERS = [
    {
        "key": "swift",
        "label": "Swift Oil",
        "codes": ["SWI03"],
        "names": ["SWIFT OIL  LEKKI", "SWIFT OIL LEKKI", "SWIFT LEKK"],
    },
    {
        "key": "chorus",
        "label": "Chorus",
        "codes": ["CHO01"],
        "names": ["CHORUS ENERGY  LEKKI", "CHORUS ENERGY LEKKI"],
    },
]

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# On Azure App Service Linux, /home is persistent storage that survives
# redeploys. Use DATA_DIR env var (set to /home/data on Azure) to keep the DB
# and generated PDFs out of wwwroot. Locally it falls back to the repo root.
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "einvoice_x3.db")
PDF_DIR = os.path.join(DATA_DIR, "invoices_x3")
os.makedirs(PDF_DIR, exist_ok=True)

PER_PAGE = 25
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "einvoice-x3-secret-key-change-me")
# Auto-reload templates when files change (so HTML edits don't require restart)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True
_db_lock = threading.Lock()

# ============================================================
# AUTHENTICATION — each user is locked to one company
# ============================================================

USERS = {
    "swift oil": {
        "password": "change@123",
        "ctx": "swift",
        "label": "Swift Oil",
    },
    "chorus": {
        "password": "Star@123",
        "ctx": "chorus",
        "label": "Chorus Energy",
    },
}

def login_required(f):
    """Decorator: redirect to /login if user is not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

def get_session_ctx():
    """Return the company filter dict for the logged-in user."""
    user = USERS.get(session.get("user", ""))
    if not user:
        return COMPANY_FILTERS[0]
    return next((f for f in COMPANY_FILTERS if f["key"] == user["ctx"]), COMPANY_FILTERS[0])

# ============================================================
# DATABASE
# ============================================================

def _open_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def db_read(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()


def db_read_one(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def db_write(sql, params=()):
    with _db_lock:
        conn = _open_db()
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()


def db_write_many(operations):
    with _db_lock:
        conn = _open_db()
        try:
            for sql, params in operations:
                conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()


def init_db():
    with _db_lock:
        conn = _open_db()
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS invoices (
                invoice_number TEXT PRIMARY KEY,
                customer_name TEXT,
                customer_id TEXT,
                customer_tin TEXT,
                customer_email TEXT,
                customer_phone TEXT,
                customer_address TEXT,
                customer_city TEXT,
                invoice_date TEXT,
                subtotal REAL DEFAULT 0,
                vat_amount REAL DEFAULT 0,
                total_amount REAL DEFAULT 0,
                currency TEXT DEFAULT 'NGN',
                status TEXT DEFAULT 'pending',
                irn TEXT,
                qr_code TEXT,
                posted_at TEXT,
                error_message TEXT,
                api_response TEXT,
                invoice_description TEXT,
                invoice_type TEXT DEFAULT 'Invoice',
                original_invoice TEXT,
                last_synced TEXT
            )""")
            # Add original_invoice column if it doesn't exist (migration)
            try:
                conn.execute("ALTER TABLE invoices ADD COLUMN original_invoice TEXT")
            except:
                pass  # Column already exists

            # Add original_invoice_irn column for storing the IRN of the original invoice
            try:
                conn.execute("ALTER TABLE invoices ADD COLUMN original_invoice_irn TEXT")
            except:
                pass  # Column already exists

            # Add original_invoice_date column for storing the issue date of the original invoice
            try:
                conn.execute("ALTER TABLE invoices ADD COLUMN original_invoice_date TEXT")
            except:
                pass  # Column already exists

            # Add company_name column (X3 sales site name, e.g. "SWIFT OIL LEKKI")
            try:
                conn.execute("ALTER TABLE invoices ADD COLUMN company_name TEXT")
            except:
                pass  # Column already exists

            # Add company_code column (X3 SALFCY, e.g. "SWI03")
            try:
                conn.execute("ALTER TABLE invoices ADD COLUMN company_code TEXT")
            except:
                pass  # Column already exists

            # Payment status for NRS: PENDING (default), PAID, REJECTED
            try:
                conn.execute("ALTER TABLE invoices ADD COLUMN payment_status TEXT DEFAULT 'PENDING'")
            except:
                pass  # Column already exists

            conn.execute("""CREATE TABLE IF NOT EXISTS invoice_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT,
                line_num INTEGER,
                item_code TEXT,
                description TEXT,
                quantity REAL DEFAULT 1,
                unit_price REAL DEFAULT 0,
                tax_rate REAL DEFAULT 0,
                amount REAL DEFAULT 0
            )""")

            conn.commit()
        finally:
            conn.close()


init_db()


# ============================================================
# SAGE X3 SYNC
# ============================================================

def sync_from_x3(date_from=None, date_to=None):
    """
    Sync invoices from Sage X3 to local database.
    """
    today = date.today()

    if not date_to:
        date_to = today.strftime("%Y-%m-%d")

    if not date_from:
        # Default to 6 months back if no date provided
        default_from = today.replace(day=1)
        for _ in range(6):
            default_from = (default_from - timedelta(days=1)).replace(day=1)
        date_from = default_from.strftime("%Y-%m-%d")

    # Connect to X3
    reader = SageX3Reader(
        base_url=X3_BASE_URL,
        folder=X3_FOLDER,
        username=X3_USERNAME,
        password=X3_PASSWORD
    )

    if not reader.connect():
        return {"ok": False, "error": "Could not connect to Sage X3. Check credentials."}

    try:
        # Fetch invoices AND credit notes from X3, restricted to allowed company codes
        invoices = reader.get_all_documents(
            from_date=date_from,
            to_date=date_to,
            limit=500,
            allowed_company_codes=ALLOWED_COMPANY_CODES or None,
        )

        if not invoices:
            return {"ok": True, "synced": 0, "new": 0, "message": "No invoices/credit notes found in date range"}

        # Get existing invoice numbers
        existing = {r["invoice_number"]: r["status"]
                    for r in db_read("SELECT invoice_number, status FROM invoices")}

        now = datetime.now().isoformat()
        operations = []
        new_count = 0

        for inv_num, inv in invoices.items():
            if inv_num in existing:
                # Update existing (don't overwrite if already posted)
                if existing[inv_num] != "posted":
                    operations.append(("""
                        UPDATE invoices SET
                            customer_name=?, customer_id=?, customer_tin=?,
                            customer_email=?, customer_phone=?, customer_address=?,
                            customer_city=?, invoice_date=?, subtotal=?, vat_amount=?,
                            total_amount=?, currency=?, invoice_description=?,
                            invoice_type=?, company_name=?, company_code=?, last_synced=?
                        WHERE invoice_number=?
                    """, (
                        inv["customer_name"], inv["customer_id"], inv["customer_tin"],
                        inv["customer_email"], inv["customer_phone"], inv["customer_address"],
                        inv["customer_city"], inv["date"], inv.get("subtotal", 0),
                        inv.get("vat_amount", 0), inv["total_amount"], inv.get("currency", "NGN"),
                        inv.get("description", ""), inv.get("invoice_type", "Invoice"),
                        inv.get("company_name", ""), inv.get("company_code", ""), now,
                        inv_num
                    )))
            else:
                # Insert new
                new_count += 1
                operations.append(("""
                    INSERT INTO invoices (
                        invoice_number, customer_name, customer_id, customer_tin,
                        customer_email, customer_phone, customer_address, customer_city,
                        invoice_date, subtotal, vat_amount, total_amount, currency,
                        status, invoice_description, invoice_type, original_invoice,
                        company_name, company_code, last_synced
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?,?,?,?)
                """, (
                    inv_num, inv["customer_name"], inv["customer_id"], inv["customer_tin"],
                    inv["customer_email"], inv["customer_phone"], inv["customer_address"],
                    inv["customer_city"], inv["date"], inv.get("subtotal", 0),
                    inv.get("vat_amount", 0), inv["total_amount"], inv.get("currency", "NGN"),
                    inv.get("description", ""), inv.get("invoice_type", "Invoice"),
                    inv.get("original_invoice", ""), inv.get("company_name", ""),
                    inv.get("company_code", ""), now
                )))

            # Sync line items
            operations.append(("DELETE FROM invoice_lines WHERE invoice_number=?", (inv_num,)))
            for i, line in enumerate(inv.get("lines", [])):
                operations.append(("""
                    INSERT INTO invoice_lines (
                        invoice_number, line_num, item_code, description,
                        quantity, unit_price, tax_rate, amount
                    ) VALUES (?,?,?,?,?,?,?,?)
                """, (
                    inv_num, i + 1, line["item_code"], line["description"],
                    line["quantity"], line["unit_price"], line.get("tax_rate", 0),
                    line.get("line_total", line["quantity"] * line["unit_price"])
                )))

        if operations:
            db_write_many(operations)

        return {
            "ok": True,
            "synced": len(invoices),
            "new": new_count,
            "date_from": date_from,
            "date_to": date_to
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        reader.close()


# ============================================================
# BUILD FIRS PAYLOAD
# ============================================================

def build_payload(invoice_number):
    """Build the Cryptware FIRS API payload for an invoice."""
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv:
        return None, [], 0, "Invoice not found"

    lines = db_read(
        "SELECT * FROM invoice_lines WHERE invoice_number=? ORDER BY line_num",
        (invoice_number,)
    )

    if not lines:
        # Fallback: create single line from total
        amt = abs(to_float(inv["total_amount"]))
        if amt > 0:
            lines = [{
                "item_code": invoice_number,
                "description": inv.get("invoice_description") or inv.get("customer_name") or "Service",
                "quantity": 1,
                "unit_price": amt,
                "tax_rate": 7.5,
                "amount": amt,
            }]
        else:
            return None, [], 0, "No line items and no amount"

    # Customer details with fallbacks
    cust_tin = inv["customer_tin"] or "00000000-0001"  # Placeholder TIN if not available
    cust_email = inv["customer_email"] or "customer@example.com"
    cust_phone = inv["customer_phone"] or "+2348000000000"

    # Calculate totals
    subtotal = sum(to_float(l.get("amount", l["quantity"] * l["unit_price"])) for l in lines)
    vat_amount = to_float(inv.get("vat_amount", 0))
    if vat_amount == 0:
        # Calculate from line tax rates
        vat_amount = sum(
            to_float(l.get("amount", 0)) * (to_float(l.get("tax_rate", 0)) / 100)
            for l in lines
        )
    grand_total = subtotal + vat_amount

    # Build API lines (Cryptware format)
    api_lines = []
    for i, line in enumerate(lines):
        qty = to_float(line.get("quantity", 1))
        price = to_float(line.get("unit_price", 0))
        if price <= 0:
            continue

        tax_rate = to_float(line.get("tax_rate", 0))
        line_ext = qty * price

        api_lines.append({
            "description": line.get("description") or "Service",
            "invoiced_quantity": qty,
            "price_amount": price,
            "hsn_code": "2710.19",  # HS code for petroleum products
            "price_unit": "LTR",  # Liters for fuel
            "product_category": "Petroleum Products",
            "base_quantity": 1,
            "tax_rate": tax_rate,
            "tax_category_id": "STANDARD_VAT" if tax_rate > 0 else "ZERO_VAT",
            "line_extension_amount": line_ext,
            "discount_rate": 0,
        })

    if not api_lines:
        return None, lines, vat_amount, "No valid line items"

    # Invoice type code (FIRS/UBL format)
    # invoice_type: "STANDARD" or "SELF_BILLED" (FIRS API requirement)
    # invoice_type_code: 380 = Credit Note, 381 = Invoice, 384 = Debit Note
    inv_type = inv.get("invoice_type") or "Invoice"
    if inv_type == "Credit Note":
        type_code = "380"  # Credit Note (UBL code)
        invoice_type = "STANDARD"  # FIRS only accepts STANDARD or SELF_BILLED
    elif inv_type == "Debit Note":
        type_code = "384"  # Debit Note (UBL code)
        invoice_type = "STANDARD"  # FIRS only accepts STANDARD or SELF_BILLED
    else:
        type_code = "381"  # Commercial Invoice (UBL code)
        invoice_type = "STANDARD"

    # Build Cryptware API payload
    payload = {
        "document_identifier": invoice_number,
        "invoice_type": invoice_type,
        "issue_date": inv["invoice_date"],
        "due_date": inv["invoice_date"],  # Same as issue date if not specified
        "invoice_type_code": type_code,
        "document_currency_code": inv.get("currency", "NGN"),
        "transaction_category": "B2B",
        "accounting_customer_party": {
            "party_name": inv["customer_name"],
            "email": cust_email,
            "telephone": cust_phone,
            "tin": cust_tin,
            "business_description": "Customer",
            "postal_address": {
                "street_name": inv["customer_address"] or "Lagos",
                "city_name": inv["customer_city"] or "Lagos",
                "postal_zone": "100001",
                "country": "NG",
            },
        },
        "invoice_lines": api_lines,
    }

    # For Credit Notes (type_code 380) or Debit Notes (384), add cancel_references
    if type_code in ("380", "384"):
        import re
        original_invoice = None
        original_irn = None
        original_issue_date = None

        # First check if we have original_invoice stored in DB (from X3 SIHORINUM field)
        if inv.get("original_invoice"):
            original_invoice = inv["original_invoice"]

        # Try to extract from description if not in DB
        if not original_invoice:
            description = inv.get("invoice_description", "") or ""

            # Pattern 1: "Credit Note for Invoice ZAINV..."
            match = re.search(r'Invoice\s+([A-Z0-9]+)', description, re.IGNORECASE)
            if match:
                original_invoice = match.group(1)

            # Pattern 2: "ZAINV..." anywhere in description
            if not original_invoice:
                match = re.search(r'(ZAINV[A-Z0-9]+)', description)
                if match:
                    original_invoice = match.group(1)

        # If no original invoice found, return error - FIRS requires it
        if not original_invoice:
            doc_type = "Credit Note" if type_code == "380" else "Debit Note"
            return None, lines, vat_amount, f"{doc_type} requires original invoice reference. Update the invoice description to include 'Invoice ZAINVXXXXXX' or set original_invoice in database."

        # First check if we have pre-stored IRN and date for the original invoice
        if inv.get("original_invoice_irn"):
            original_irn = inv["original_invoice_irn"]
            original_issue_date = inv.get("original_invoice_date") or inv["invoice_date"]
        else:
            # Try to get the IRN of the original invoice from our database
            original_inv = db_read_one(
                "SELECT irn, invoice_date FROM invoices WHERE invoice_number=?",
                (original_invoice,)
            )
            if original_inv and original_inv.get("irn"):
                original_irn = original_inv["irn"]
                original_issue_date = original_inv.get("invoice_date", inv["invoice_date"])

                # Store the IRN and date for future use
                db_write(
                    "UPDATE invoices SET original_invoice_irn=?, original_invoice_date=? WHERE invoice_number=?",
                    (original_irn, original_issue_date, inv["invoice_number"])
                )
            else:
                # If original invoice not found in DB or not posted, use placeholder
                # The IRN format from FIRS is typically: INV-YYYY-XXX-HASH-YYYYMMDD
                original_irn = original_invoice  # Use the invoice number as reference
                original_issue_date = inv["invoice_date"]  # Use credit note date as fallback

        # FIRS requires cancel_references array for credit/debit notes
        payload["cancel_references"] = [
            {
                "original_irn": original_irn,
                "original_issue_date": original_issue_date
            }
        ]

    return payload, lines, vat_amount, None


# ============================================================
# POST TO FIRS
# ============================================================

def _update_payment_status(api_url, headers, irn, payment_status):
    """Call PATCH /invoice/{irn} to update payment status at NRS.

    Args:
        payment_status: "PAID" or "REJECTED"

    This is a ONE-TIME, IRREVERSIBLE operation at NRS.
    """
    try:
        r = requests.patch(
            f"{api_url}/invoice/{irn}",
            headers=headers,
            json={"payment_status": payment_status, "reference": irn},
            timeout=30,
        )
        ok = r.status_code in (200, 204)
        return ok, r.status_code, r.text[:500]
    except Exception as e:
        return False, 0, f"PATCH error: {e}"


def post_to_firs(invoice_number):
    """Post an invoice to FIRS via Cryptware API.

    Resolves the API URL + key + supplier profile per-company so Swift Oil
    and Chorus invoices go to their own accounts.
    """
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv:
        return {"ok": False, "error": "Not found"}
    if inv["status"] == "posted":
        return {"ok": False, "error": "Already posted", "irn": inv["irn"]}

    # Resolve which FIRS account this invoice belongs to
    company_code = resolve_company_code(inv)
    api_url, api_key, _supplier, _logo = get_company_api(company_code)
    if not api_key:
        msg = f"No FIRS API key configured for company {company_code or '(unknown)'}"
        db_write(
            "UPDATE invoices SET status='failed', error_message=? WHERE invoice_number=?",
            (msg[:500], invoice_number),
        )
        return {"ok": False, "error": msg}

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }

    payload, lines, vat_amount, build_error = build_payload(invoice_number)
    if not payload:
        db_write(
            "UPDATE invoices SET status='failed', error_message=? WHERE invoice_number=?",
            (build_error[:500], invoice_number)
        )
        return {"ok": False, "error": build_error}

    # Update VAT in DB
    db_write(
        "UPDATE invoices SET vat_amount=? WHERE invoice_number=?",
        (vat_amount, invoice_number)
    )

    try:
        # Cryptware API endpoint: POST /invoice/generate
        resp = requests.post(
            f"{api_url}/invoice/generate",
            headers=headers,
            json=payload,
            timeout=60
        )
        resp_text = resp.text
        resp_json = {}
        try:
            resp_json = resp.json()
        except:
            pass

        if resp.status_code in (200, 201):
            data = resp_json.get("data", resp_json)
            irn = data.get("irn", "N/A")
            qr_code = data.get("qr_code_url", "")  # Cryptware uses qr_code_url
            inv_id = data.get("id", "")

            db_write("""
                UPDATE invoices SET
                    status='posted', irn=?, qr_code=?, posted_at=?,
                    payment_status='PENDING',
                    error_message=NULL, api_response=?
                WHERE invoice_number=?
            """, (irn, qr_code, datetime.now().isoformat(), resp_text[:5000], invoice_number))

            generate_pdf(invoice_number)
            return {"ok": True, "irn": irn, "status": "posted", "id": inv_id}

        elif resp.status_code == 409:
            # Duplicate - already exists
            error_msg = resp_json.get("message", "Invoice already exists")
            # Try to extract IRN from response
            data = resp_json.get("data", {})
            irn = data.get("irn", "")
            qr_code = data.get("qr_code_url", "")

            if irn:
                db_write("""
                    UPDATE invoices SET
                        status='posted', irn=?, qr_code=?, posted_at=?,
                        payment_status='PENDING',
                        error_message=NULL, api_response=?
                    WHERE invoice_number=?
                """, (irn, qr_code, datetime.now().isoformat(), resp_text[:5000], invoice_number))

                generate_pdf(invoice_number)
                return {"ok": True, "irn": irn, "status": "posted", "note": "Already exists"}

            db_write(
                "UPDATE invoices SET status='failed', error_message=?, api_response=? WHERE invoice_number=?",
                (error_msg[:500], resp_text[:5000], invoice_number)
            )
            return {"ok": False, "error": error_msg, "status_code": 409}

        elif resp.status_code == 400:
            # Validation error
            error_msg = resp_json.get("message", "Validation failed")
            errors = resp_json.get("errors", [])
            if errors:
                error_msg = f"{error_msg}: {errors}"
            db_write(
                "UPDATE invoices SET status='failed', error_message=?, api_response=? WHERE invoice_number=?",
                (str(error_msg)[:500], resp_text[:5000], invoice_number)
            )
            return {"ok": False, "error": error_msg, "status_code": 400}

        elif resp.status_code == 422:
            # NRS validation failed
            error_msg = resp_json.get("message", "NRS validation failed")
            db_write(
                "UPDATE invoices SET status='failed', error_message=?, api_response=? WHERE invoice_number=?",
                (error_msg[:500], resp_text[:5000], invoice_number)
            )
            return {"ok": False, "error": error_msg, "status_code": 422}

        else:
            error_msg = resp_json.get("message", resp.text[:300])
            db_write(
                "UPDATE invoices SET status='failed', error_message=?, api_response=? WHERE invoice_number=?",
                (error_msg[:500], resp_text[:5000], invoice_number)
            )
            return {"ok": False, "error": error_msg, "status_code": resp.status_code}

    except requests.exceptions.ConnectionError as e:
        db_write(
            "UPDATE invoices SET status='failed', error_message=? WHERE invoice_number=?",
            (f"Connection: {str(e)[:200]}", invoice_number)
        )
        return {"ok": False, "error": f"Connection failed: {e}"}
    except Exception as e:
        db_write(
            "UPDATE invoices SET status='failed', error_message=? WHERE invoice_number=?",
            (str(e)[:500], invoice_number)
        )
        return {"ok": False, "error": str(e)}


# ============================================================
# PDF GENERATION
# ============================================================

def generate_pdf(invoice_number):
    """Generate PDF invoice with QR code."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.pdfgen import canvas
    from reportlab.platypus import Table, TableStyle
    from reportlab.lib.utils import ImageReader

    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    lines = db_read(
        "SELECT * FROM invoice_lines WHERE invoice_number=? ORDER BY line_num",
        (invoice_number,)
    )
    if not inv:
        return None

    # Look up the supplier profile for this invoice's company
    _, _, supplier, logo_path = get_company_api(resolve_company_code(inv))
    supplier_address = ", ".join(
        p for p in [supplier.get("street_name"), supplier.get("city_name")] if p
    ) or "Lagos, Nigeria"

    # Download QR code image from URL (Cryptware returns a Cloudinary image URL)
    qr_img_reader = None
    if inv.get("qr_code"):
        qr_url = inv["qr_code"]
        try:
            # If it's a URL, download the image
            if qr_url.startswith("http"):
                resp = requests.get(qr_url, timeout=10)
                if resp.status_code == 200:
                    buf = io.BytesIO(resp.content)
                    qr_img_reader = ImageReader(buf)
            else:
                # Fallback: generate QR code from data string
                import qrcode
                qr = qrcode.QRCode(version=1, box_size=4, border=2)
                qr.add_data(qr_url)
                qr.make(fit=True)
                img = qr.make_image(fill_color="black", back_color="white")
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                buf.seek(0)
                qr_img_reader = ImageReader(buf)
        except Exception as e:
            print(f"Warning: Could not load QR code: {e}")

    # Safe filename
    safe_name = invoice_number.replace("/", "_").replace("\\", "_").replace(" ", "_")
    pdf_path = os.path.join(PDF_DIR, f"{safe_name}.pdf")

    w, h = A4
    c = canvas.Canvas(pdf_path, pagesize=A4)

    # Colors
    navy = colors.HexColor("#0f172a")
    blue = colors.HexColor("#2563eb")
    slate50 = colors.HexColor("#f8fafc")
    slate200 = colors.HexColor("#e2e8f0")
    slate500 = colors.HexColor("#64748b")
    slate800 = colors.HexColor("#1e293b")
    green = colors.HexColor("#16a34a")

    y = h - 30

    # Header
    c.setFillColor(navy)
    c.rect(0, y - 60, w, 70, fill=True, stroke=False)

    # Company logo (left side of header)
    logo_x_offset = 30
    if logo_path and os.path.exists(logo_path):
        try:
            logo_img = ImageReader(logo_path)
            logo_h = 50
            iw, ih = logo_img.getSize()
            logo_w = logo_h * (iw / ih)  # maintain aspect ratio
            c.drawImage(logo_img, 30, y - 55, width=logo_w, height=logo_h, mask="auto")
            logo_x_offset = 30 + logo_w + 10  # shift text right of logo
        except Exception as e:
            print(f"Warning: Could not load logo: {e}")

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(logo_x_offset, y - 25, supplier.get("name", ""))
    c.setFont("Helvetica", 9)
    c.drawString(logo_x_offset, y - 42, supplier_address)

    # E-Invoice badge
    c.setFillColor(green)
    c.roundRect(w - 145, y - 47, 115, 30, 4, fill=True, stroke=False)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(w - 87, y - 37, "E-INVOICE")

    y -= 85

    # Invoice title
    c.setFillColor(slate800)
    c.setFont("Helvetica-Bold", 22)
    c.drawString(30, y, "INVOICE")
    y -= 25

    # Invoice details
    for label, val in [
        ("Invoice No:", inv["invoice_number"]),
        ("Date:", inv["invoice_date"]),
        ("IRN:", inv.get("irn") or "Pending"),
        ("Currency:", inv.get("currency", "NGN"))
    ]:
        c.setFont("Helvetica-Bold", 9)
        c.setFillColor(slate500)
        c.drawString(30, y, label)
        c.setFont("Helvetica", 9)
        c.setFillColor(slate800)
        c.drawString(115, y, str(val))
        y -= 15

    # QR Code
    if qr_img_reader:
        c.drawImage(qr_img_reader, w - 140, y + 5, 105, 105)

    y -= 15

    # Customer box
    c.setFillColor(slate50)
    c.rect(25, y - 55, w - 50, 60, fill=True, stroke=False)
    c.setStrokeColor(slate200)
    c.rect(25, y - 55, w - 50, 60, fill=False, stroke=True)

    c.setFillColor(blue)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(35, y - 5, "BILL TO")

    c.setFillColor(slate800)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(35, y - 20, inv["customer_name"] or "")

    c.setFont("Helvetica", 8)
    c.setFillColor(slate500)
    addr = f"{inv['customer_address'] or ''}, {inv['customer_city'] or ''}".strip(", ")
    c.drawString(35, y - 34, addr[:80])

    if inv["customer_tin"]:
        c.drawString(35, y - 46, f"TIN: {inv['customer_tin']}")

    c.drawRightString(w - 35, y - 20, inv["customer_email"] or "")
    c.drawRightString(w - 35, y - 34, inv["customer_phone"] or "")

    y -= 75

    # Line items table
    c.setFillColor(slate800)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(30, y, "Line Items")
    y -= 5

    table_data = [["#", "Description", "Qty", "Unit Price", "Tax", "Amount"]]
    total = 0.0

    for line in lines:
        qty = to_float(line["quantity"])
        price = to_float(line["unit_price"])
        amt = qty * price
        total += amt
        tax_rate = to_float(line.get("tax_rate", 0))
        tax_label = f"{tax_rate:g}%" if tax_rate > 0 else "0%"

        table_data.append([
            str(line["line_num"]),
            (line["description"] or "Service")[:40],
            f"{qty:g}",
            f"{price:,.2f}",
            tax_label,
            f"{amt:,.2f}"
        ])

    col_widths = [25, 220, 35, 85, 40, 85]

    t = Table(table_data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), navy),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 7.5),
        ("TEXTCOLOR", (0, 1), (-1, -1), slate800),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
        ("LINEBELOW", (0, 0), (-1, 0), 1, navy),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, slate200),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))

    tw, th = t.wrap(0, 0)
    t.drawOn(c, 30, y - th)
    y -= th + 20

    # Totals
    vat_amount = to_float(inv.get("vat_amount", 0))
    grand = total + vat_amount
    tx = w - 230
    bw = 200

    c.setFillColor(slate50)
    c.rect(tx, y - 65, bw, 70, fill=True, stroke=False)
    c.setStrokeColor(slate200)
    c.rect(tx, y - 65, bw, 70, fill=False, stroke=True)

    c.setFont("Helvetica", 9)
    c.setFillColor(slate500)
    c.drawString(tx + 10, y - 8, "Subtotal:")
    c.drawString(tx + 10, y - 23, "VAT:")

    c.setFillColor(slate800)
    c.drawRightString(tx + bw - 10, y - 8, f"N{total:,.2f}")
    c.drawRightString(tx + bw - 10, y - 23, f"N{vat_amount:,.2f}")

    c.setStrokeColor(navy)
    c.line(tx + 10, y - 33, tx + bw - 10, y - 33)

    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(navy)
    c.drawString(tx + 10, y - 50, "TOTAL:")
    c.drawRightString(tx + bw - 10, y - 50, f"N{grand:,.2f}")

    # Footer
    c.setFillColor(navy)
    c.rect(0, 0, w, 45, fill=True, stroke=False)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(30, 28, f"IRN: {inv.get('irn') or 'Pending'}")
    c.setFont("Helvetica", 7)
    c.drawString(30, 15, "System-generated e-invoice. Validated by Nigeria E-Invoicing Portal (FIRS).")

    c.save()
    return pdf_path


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""
        user = USERS.get(username)
        if user and user["password"] == password:
            session["user"] = username
            return redirect(url_for("index"))
        error = "Invalid username or password"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    page = request.args.get("page", 1, type=int)
    # Company context is locked to the logged-in user
    ctx_filter = get_session_ctx()
    ctx = ctx_filter["key"]
    status = (request.args.get("status") or "").strip().lower()
    q = (request.args.get("q") or "").strip()

    # Build the company WHERE clause (always active — scoped by header tab)
    where_parts = []
    params = []

    or_parts = []
    if ctx_filter.get("codes"):
        code_ph = ",".join("?" * len(ctx_filter["codes"]))
        or_parts.append(f"company_code IN ({code_ph})")
        params.extend(ctx_filter["codes"])
    if ctx_filter.get("names"):
        name_ph = ",".join("?" * len(ctx_filter["names"]))
        or_parts.append(f"company_name IN ({name_ph})")
        params.extend(ctx_filter["names"])
    if or_parts:
        where_parts.append("(" + " OR ".join(or_parts) + ")")

    # Stats scoped to the active company
    try:
        where_sql_stats = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        all_stats = db_read(
            f"SELECT status, COUNT(*) as cnt FROM invoices{where_sql_stats} GROUP BY status",
            tuple(params),
        )
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
        for s in all_stats:
            stats[s["status"]] = s["cnt"]
            stats["total"] += s["cnt"]
    except:
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}

    # Additional filters (status, search) on top of company scope
    if status in ("pending", "posted", "failed"):
        where_parts.append("status = ?")
        params.append(status)

    if q:
        where_parts.append("(invoice_number LIKE ? OR customer_name LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like])

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    try:
        total_row = db_read_one(
            f"SELECT COUNT(*) as cnt FROM invoices{where_sql}",
            tuple(params),
        )
        total = total_row["cnt"] if total_row else 0
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PER_PAGE

        invoices = db_read(
            f"SELECT * FROM invoices{where_sql} "
            f"ORDER BY invoice_date DESC, invoice_number DESC LIMIT ? OFFSET ?",
            tuple(params) + (PER_PAGE, offset),
        )
    except Exception as e:
        print(f"[index] query error: {e}")
        invoices = []
        total = 0
        total_pages = 1
        page = 1

    # Default date range for sync (last 3 months)
    today = date.today()
    default_from = today.replace(day=1)
    # Go back 3 months
    for _ in range(3):
        default_from = (default_from - timedelta(days=1)).replace(day=1)

    return render_template(
        "index_x3.html",
        invoices=invoices,
        stats=stats,
        page=page,
        total_pages=total_pages,
        total=total,
        default_from=default_from.strftime("%Y-%m-%d"),
        default_to=today.strftime("%Y-%m-%d"),
        company_filters=COMPANY_FILTERS,
        active_ctx=ctx,
        active_ctx_label=ctx_filter["label"],
        firs_env_label="PREPROD" if FIRS_ENV == "test" else "PRODUCTION",
        filter_status=status,
        filter_q=q,
    )


@app.route("/api/sync", methods=["POST"])
@login_required
def api_sync():
    data = request.get_json(silent=True) or {}
    return jsonify(sync_from_x3(
        date_from=data.get("date_from"),
        date_to=data.get("date_to")
    ))


@app.route("/api/post/<invoice_number>", methods=["POST"])
@login_required
def api_post(invoice_number):
    return jsonify(post_to_firs(invoice_number))


@app.route("/api/preview-payload/<invoice_number>")
@login_required
def api_preview_payload(invoice_number):
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv:
        return jsonify({"ok": False, "error": "Invoice not found"})

    payload, lines, vat_amount, error = build_payload(invoice_number)
    if not payload:
        return jsonify({"ok": False, "error": error or "Failed to build payload"})

    company_code = resolve_company_code(inv)
    api_url, _api_key, supplier, _logo = get_company_api(company_code)

    subtotal = sum(to_float(l.get("amount", 0)) for l in lines)
    return jsonify({
        "ok": True,
        "invoice_number": inv["invoice_number"],
        "customer_name": inv["customer_name"],
        "company_code": company_code,
        "company_label": (COMPANY_API_CONFIG.get(company_code) or {}).get("name") or "Default",
        "firs_env": FIRS_ENV,
        "subtotal": subtotal,
        "vat_amount": vat_amount,
        "grand_total": subtotal + vat_amount,
        "lines_count": len(lines),
        "api_url": f"{api_url}/invoice/generate",
        "supplier_name": supplier.get("name"),
        "payload": payload
    })


@app.route("/api/error-details/<invoice_number>")
@login_required
def api_error_details(invoice_number):
    inv = db_read_one(
        "SELECT invoice_number, customer_name, status, error_message, api_response FROM invoices WHERE invoice_number=?",
        (invoice_number,)
    )
    if not inv:
        return jsonify({"ok": False, "error": "Invoice not found"})

    api_resp = inv.get("api_response") or ""
    parsed = None
    try:
        parsed = json.loads(api_resp)
    except:
        pass

    return jsonify({
        "ok": True,
        "invoice_number": invoice_number,
        "customer_name": inv["customer_name"],
        "status": inv["status"],
        "error_message": inv["error_message"] or "",
        "api_response": parsed or api_resp
    })


@app.route("/api/post-bulk", methods=["POST"])
@login_required
def api_post_bulk():
    """Post a user-selected list of invoices to FIRS.

    Body: {"invoice_numbers": ["ZAINV...", "ZAINV..."]}
    Only invoices currently in 'pending' or 'failed' status are attempted.
    """
    data = request.get_json(silent=True) or {}
    selected = data.get("invoice_numbers") or []
    if not isinstance(selected, list) or not selected:
        return jsonify({"ok": False, "error": "No invoices selected"}), 400

    # Filter to only pending/failed so already-posted invoices aren't re-submitted
    placeholders = ",".join("?" * len(selected))
    rows = db_read(
        f"SELECT invoice_number FROM invoices "
        f"WHERE invoice_number IN ({placeholders}) AND status IN ('pending','failed')",
        tuple(selected),
    )

    results = []
    for row in rows:
        results.append({
            "invoice_number": row["invoice_number"],
            **post_to_firs(row["invoice_number"])
        })
    posted = sum(1 for r in results if r.get("ok"))
    return jsonify({
        "ok": True,
        "posted": posted,
        "failed": len(results) - posted,
        "skipped": len(selected) - len(results),
        "details": results
    })


@app.route("/api/payment-status/<invoice_number>", methods=["POST"])
@login_required
def api_update_payment_status(invoice_number):
    """Update payment status (PAID / REJECTED) at NRS via PATCH /invoice/{irn}.

    This is a ONE-TIME, IRREVERSIBLE operation. Once set, cannot be changed.
    Body: {"payment_status": "PAID"} or {"payment_status": "REJECTED"}
    """
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv:
        return jsonify({"ok": False, "error": "Invoice not found"}), 404

    if inv["status"] != "posted":
        return jsonify({"ok": False, "error": "Invoice must be posted first"}), 400

    current_ps = (inv.get("payment_status") or "PENDING").upper()
    if current_ps in ("PAID", "REJECTED"):
        return jsonify({"ok": False, "error": f"Payment status already set to {current_ps}. This cannot be changed."}), 400

    irn = inv.get("irn")
    if not irn or irn == "N/A":
        return jsonify({"ok": False, "error": "No valid IRN — cannot update payment status"}), 400

    data = request.get_json(silent=True) or {}
    new_status = (data.get("payment_status") or "").upper()
    if new_status not in ("PAID", "REJECTED"):
        return jsonify({"ok": False, "error": "payment_status must be PAID or REJECTED"}), 400

    # Resolve the company's API creds
    company_code = resolve_company_code(inv)
    api_url, api_key, _, _ = get_company_api(company_code)
    if not api_key:
        return jsonify({"ok": False, "error": f"No API key for company {company_code}"}), 400

    headers = {"Content-Type": "application/json", "x-api-key": api_key}

    ok, status_code, body = _update_payment_status(api_url, headers, irn, new_status)

    if ok:
        db_write(
            "UPDATE invoices SET payment_status=? WHERE invoice_number=?",
            (new_status, invoice_number),
        )
        return jsonify({"ok": True, "payment_status": new_status, "irn": irn})
    else:
        return jsonify({
            "ok": False,
            "error": f"NRS returned {status_code}: {body}",
            "status_code": status_code,
        }), 400


@app.route("/api/stats")
@login_required
def api_stats():
    try:
        all_stats = db_read("SELECT status, COUNT(*) as cnt FROM invoices GROUP BY status")
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
        for s in all_stats:
            stats[s["status"]] = s["cnt"]
            stats["total"] += s["cnt"]
        return jsonify({"ok": True, **stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/test-x3")
@login_required
def api_test_x3():
    """Test Sage X3 connection."""
    reader = SageX3Reader(
        base_url=X3_BASE_URL,
        folder=X3_FOLDER,
        username=X3_USERNAME,
        password=X3_PASSWORD
    )

    if reader.connect():
        # Try to fetch some data
        try:
            endpoints = reader.test_endpoints()
            reader.close()
            return jsonify({
                "ok": True,
                "message": f"Connected to X3 folder: {X3_FOLDER}",
                "base_url": X3_BASE_URL,
                "endpoints": endpoints
            })
        except Exception as e:
            reader.close()
            return jsonify({"ok": False, "error": str(e)})
    else:
        return jsonify({
            "ok": False,
            "error": "Could not connect to Sage X3",
            "base_url": X3_BASE_URL,
            "folder": X3_FOLDER
        })


@app.route("/download/<invoice_number>")
@login_required
def download_pdf(invoice_number):
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv or inv["status"] != "posted":
        return "Not posted yet", 404

    safe_name = invoice_number.replace("/", "_").replace("\\", "_").replace(" ", "_")
    pdf_path = os.path.join(PDF_DIR, f"{safe_name}.pdf")

    if not os.path.exists(pdf_path):
        generate_pdf(invoice_number)

    if os.path.exists(pdf_path):
        return send_file(pdf_path, as_attachment=True, download_name=f"{safe_name}.pdf")

    return "PDF generation failed", 500


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  Nigeria E-Invoicing Dashboard (Sage X3)")
    print("=" * 50)
    print(f"  X3 Server: {X3_BASE_URL}")
    print(f"  X3 Folder: {X3_FOLDER}")
    print(f"  Dashboard: http://localhost:5001")
    print("=" * 50)

    # Warn if credentials not set
    if not X3_PASSWORD:
        print("\n  WARNING: X3_PASSWORD not set!")
        print("  Set environment variable or edit app_x3.py")

    # Per-company FIRS key status
    print(f"\n  FIRS env: {FIRS_ENV}")
    for code, cfg in COMPANY_API_CONFIG.items():
        env_cfg = cfg.get(FIRS_ENV) or {}
        has_key = bool(env_cfg.get("key"))
        mark = "[OK]" if has_key else "[--]"
        print(f"  {mark} {code:<6} {cfg['name']:<20} {env_cfg.get('url','(no url)')}")
    if not any((cfg.get(FIRS_ENV) or {}).get("key") for cfg in COMPANY_API_CONFIG.values()):
        print("\n  WARNING: No per-company FIRS keys are set for this FIRS_ENV!")
        print("  Check SWIFT_FIRS_*_KEY and CHORUS_FIRS_*_KEY in .env")

    # Show current DB state (no auto-sync - data is persistent)
    try:
        total_row = db_read_one("SELECT COUNT(*) as cnt FROM invoices")
        print(f"\n  DB: {total_row['cnt'] if total_row else 0} invoices loaded from einvoice_x3.db")
        print("      Click 'Sync from X3' in the dashboard to fetch new invoices.")
    except Exception as e:
        print(f"\n  [WARN] Could not read DB: {e}")

    print("\n" + "=" * 50)
    print("  Starting web server...")
    print("=" * 50 + "\n")

    app.run(debug=False, host="0.0.0.0", port=5001)
