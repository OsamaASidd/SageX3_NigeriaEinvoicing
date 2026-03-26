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
from flask import Flask, render_template, jsonify, send_file, request

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from sage_x3_reader import SageX3Reader, to_float, to_str
from config_x3 import (
    X3_BASE_URL, X3_FOLDER, X3_USERNAME, X3_PASSWORD,
    API_BASE_URL, API_KEY, PARTICIPANT_ID
)

# ============================================================
# CONFIGURATION - Update these for Swift Oil
# ============================================================
# X3 Connection loaded from config_x3.py

# FIRS API (Cryptware Systems)
API_URL = API_BASE_URL
API_HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": API_KEY,
}

# Supplier Info (Swift Oil)
SUPPLIER = {
    "name": os.environ.get("SUPPLIER_NAME", "SWIFT OIL LIMITED"),
    "address": "Lagos, Nigeria",
    "tin": os.environ.get("SUPPLIER_TIN", ""),  # SET THIS!
    "email": "info@swiftoil.com",
    "telephone": "+234",
    "business_id": API_HEADERS.get("participant-id", ""),
    "street_name": "Lagos",
    "city_name": "Lagos",
    "postal_zone": "100001",
    "country": "NG",
}

# Tax categories
TAX_CAT_STANDARD = os.environ.get("TAX_CAT_STANDARD", "STANDARD_VAT")
TAX_CAT_EXEMPT = os.environ.get("TAX_CAT_EXEMPT", "ZERO_VAT")

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "einvoice_x3.db")
PDF_DIR = os.path.join(BASE_DIR, "invoices_x3")
os.makedirs(PDF_DIR, exist_ok=True)

PER_PAGE = 25
app = Flask(__name__)
_db_lock = threading.Lock()

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
        # Fetch invoices AND credit notes from X3
        invoices = reader.get_all_documents(from_date=date_from, to_date=date_to, limit=500)

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
                            invoice_type=?, last_synced=?
                        WHERE invoice_number=?
                    """, (
                        inv["customer_name"], inv["customer_id"], inv["customer_tin"],
                        inv["customer_email"], inv["customer_phone"], inv["customer_address"],
                        inv["customer_city"], inv["date"], inv.get("subtotal", 0),
                        inv.get("vat_amount", 0), inv["total_amount"], inv.get("currency", "NGN"),
                        inv.get("description", ""), inv.get("invoice_type", "Invoice"), now,
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
                        status, invoice_description, invoice_type, original_invoice, last_synced
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?,?,?,?)
                """, (
                    inv_num, inv["customer_name"], inv["customer_id"], inv["customer_tin"],
                    inv["customer_email"], inv["customer_phone"], inv["customer_address"],
                    inv["customer_city"], inv["date"], inv.get("subtotal", 0),
                    inv.get("vat_amount", 0), inv["total_amount"], inv.get("currency", "NGN"),
                    inv.get("description", ""), inv.get("invoice_type", "Invoice"),
                    inv.get("original_invoice", ""), now
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

def post_to_firs(invoice_number):
    """Post an invoice to FIRS via Cryptware API."""
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv:
        return {"ok": False, "error": "Not found"}
    if inv["status"] == "posted":
        return {"ok": False, "error": "Already posted", "irn": inv["irn"]}

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
            f"{API_URL}/invoice/generate",
            headers=API_HEADERS,
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
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(30, y - 25, SUPPLIER["name"])
    c.setFont("Helvetica", 9)
    c.drawString(30, y - 42, SUPPLIER["address"])

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

@app.route("/")
def index():
    page = request.args.get("page", 1, type=int)
    try:
        total_row = db_read_one("SELECT COUNT(*) as cnt FROM invoices")
        total = total_row["cnt"] if total_row else 0
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PER_PAGE

        invoices = db_read(
            "SELECT * FROM invoices ORDER BY invoice_date DESC, invoice_number DESC LIMIT ? OFFSET ?",
            (PER_PAGE, offset)
        )

        all_stats = db_read("SELECT status, COUNT(*) as cnt FROM invoices GROUP BY status")
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
        for s in all_stats:
            stats[s["status"]] = s["cnt"]
            stats["total"] += s["cnt"]

    except:
        invoices = []
        stats = {"total": 0, "posted": 0, "pending": 0, "failed": 0}
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
        default_to=today.strftime("%Y-%m-%d")
    )


@app.route("/api/sync", methods=["POST"])
def api_sync():
    data = request.get_json(silent=True) or {}
    return jsonify(sync_from_x3(
        date_from=data.get("date_from"),
        date_to=data.get("date_to")
    ))


@app.route("/api/post/<invoice_number>", methods=["POST"])
def api_post(invoice_number):
    return jsonify(post_to_firs(invoice_number))


@app.route("/api/preview-payload/<invoice_number>")
def api_preview_payload(invoice_number):
    inv = db_read_one("SELECT * FROM invoices WHERE invoice_number=?", (invoice_number,))
    if not inv:
        return jsonify({"ok": False, "error": "Invoice not found"})

    payload, lines, vat_amount, error = build_payload(invoice_number)
    if not payload:
        return jsonify({"ok": False, "error": error or "Failed to build payload"})

    subtotal = sum(to_float(l.get("amount", 0)) for l in lines)
    return jsonify({
        "ok": True,
        "invoice_number": inv["invoice_number"],
        "customer_name": inv["customer_name"],
        "subtotal": subtotal,
        "vat_amount": vat_amount,
        "grand_total": subtotal + vat_amount,
        "lines_count": len(lines),
        "api_url": f"{API_URL}/invoice/generate",
        "payload": payload
    })


@app.route("/api/error-details/<invoice_number>")
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
def api_post_bulk():
    pending = db_read("SELECT invoice_number FROM invoices WHERE status='pending'")
    results = []
    for row in pending:
        results.append({
            "invoice_number": row["invoice_number"],
            **post_to_firs(row["invoice_number"])
        })
    posted = sum(1 for r in results if r.get("ok"))
    return jsonify({
        "ok": True,
        "posted": posted,
        "failed": len(results) - posted,
        "details": results
    })


@app.route("/api/stats")
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

def preload_data():
    """Preload invoices and credit notes from Sage X3 on startup."""
    print("\n  Preloading data from Sage X3...")

    try:
        result = sync_from_x3(date_from="2024-01-01")
        if result.get("ok"):
            total = result.get("synced", 0)
            new = result.get("new", 0)
            print(f"  [OK] Synced {total} documents ({new} new)")

            # Show summary
            stats = db_read("SELECT invoice_type, COUNT(*) as cnt FROM invoices GROUP BY invoice_type")
            for s in stats:
                print(f"       - {s['invoice_type']}: {s['cnt']}")
        else:
            print(f"  [WARN] Sync issue: {result.get('error', 'Unknown')}")
    except Exception as e:
        print(f"  [WARN] Could not preload: {e}")
        print("         Data will be loaded when you click 'Sync from X3'")


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

    if not API_HEADERS.get("x-api-key"):
        print("\n  WARNING: FIRS_API_KEY not set!")
        print("  Set environment variable for FIRS posting")

    # Preload invoices and credit notes from X3
    preload_data()

    print("\n" + "=" * 50)
    print("  Starting web server...")
    print("=" * 50 + "\n")

    app.run(debug=False, host="0.0.0.0", port=5001)
