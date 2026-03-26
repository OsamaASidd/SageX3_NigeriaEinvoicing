"""
Sage X3 Configuration
=====================
Configuration for Sage X3 integration with FIRS E-Invoicing.

All sensitive values are loaded from environment variables.
Set these in your environment or in a .env file (not committed to git).

Required environment variables:
    X3_BASE_URL      - Sage X3 server URL (e.g., http://server:8124)
    X3_FOLDER        - X3 folder/pool name (e.g., SwiftOil)
    X3_USERNAME      - X3 web services username
    X3_PASSWORD      - X3 web services password
    FIRS_API_KEY     - Cryptware FIRS API key
"""

import os

# ============================================================
# SAGE X3 CONNECTION SETTINGS
# ============================================================

# X3 Syracuse Server (REST API)
X3_BASE_URL = os.environ.get("X3_BASE_URL", "")

# X3 Folder/Endpoint (Pool alias)
X3_FOLDER = os.environ.get("X3_FOLDER", "")

# X3 Web Services Authentication
X3_USERNAME = os.environ.get("X3_USERNAME", "")
X3_PASSWORD = os.environ.get("X3_PASSWORD", "")

# Alternative: Direct SQL Server Connection
X3_SQL_SERVER = os.environ.get("X3_SQL_SERVER", "")
X3_SQL_DATABASE = os.environ.get("X3_SQL_DATABASE", "x3")
X3_SQL_USER = os.environ.get("X3_SQL_USER", "")
X3_SQL_PASSWORD = os.environ.get("X3_SQL_PASSWORD", "")

# ============================================================
# FIRS E-INVOICING API (Cryptware Systems)
# ============================================================

# API URL: Use preprod for testing, api for production
# Test: https://preprod-api.cryptwaresystemsltd.com
# Prod: https://api.cryptwaresystemsltd.com
API_BASE_URL = os.environ.get("FIRS_API_URL", "https://preprod-api.cryptwaresystemsltd.com")
API_KEY = os.environ.get("FIRS_API_KEY", "")

# Participant details
PARTICIPANT_ID = os.environ.get("FIRS_PARTICIPANT_ID", "")

# ============================================================
# SUPPLIER INFO
# ============================================================

SUPPLIER = {
    "name": os.environ.get("SUPPLIER_NAME", "YOUR COMPANY NAME"),
    "tin": os.environ.get("SUPPLIER_TIN", ""),
    "email": os.environ.get("SUPPLIER_EMAIL", ""),
    "telephone": os.environ.get("SUPPLIER_PHONE", "+234"),
    "business_description": os.environ.get("SUPPLIER_DESCRIPTION", ""),
    "street_name": os.environ.get("SUPPLIER_STREET", ""),
    "city_name": os.environ.get("SUPPLIER_CITY", "Lagos"),
    "postal_zone": os.environ.get("SUPPLIER_POSTAL", "100001"),
    "country": "NG",
    "business_id": PARTICIPANT_ID,
}

# ============================================================
# DEFAULT VALUES
# ============================================================

DEFAULT_CURRENCY = "NGN"
DEFAULT_TAX_RATE = 7.5
DEFAULT_TAX_CATEGORY = "STANDARD_VAT"
DEFAULT_TAX_CATEGORY_EXEMPT = "ZERO_VAT"
DEFAULT_UOM = "EA"
DEFAULT_COUNTRY = "NG"

# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "einvoice_x3.db")
PDF_DIR = os.path.join(BASE_DIR, "invoices_x3")

# Mapping files
CUSTOMER_TIN_MAP_FILE = os.path.join(BASE_DIR, "mappings", "customer_tin_map_x3.csv")
HSN_CODE_MAP_FILE = os.path.join(BASE_DIR, "mappings", "hsn_code_map.csv")
PRODUCT_CATEGORY_MAP_FILE = os.path.join(BASE_DIR, "mappings", "product_category_map.csv")
