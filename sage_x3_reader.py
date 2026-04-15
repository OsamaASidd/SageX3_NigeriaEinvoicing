"""
Sage X3 Data Reader
====================
Reads invoice data from Sage X3 via SOAP Web Services.

SAGE X3 SOAP API:
    WSDL URL: http://{server}:{port}/soap-wsdl/syracuse/collaboration/syracuse/CAdxWebServiceXmlCC?wsdl
    Service URL: http://{server}:{port}/soap-generic/syracuse/collaboration/syracuse/CAdxWebServiceXmlCC

    Methods:
    - query: List records (LIST)
    - read: Get single record with full details
    - save: Create/Update records

AUTHENTICATION:
    - Basic Auth: Base64(username:password) in Authorization header
    - Must be configured in X3's nodelocal.js
    - User must be mapped to X3 user with appropriate security profile

KEY PUBLICATIONS:
    XSIH        - Sales Invoice (custom publication)
    BPCUSTOMER  - Business Partner (Customer)
    ITMMASTER   - Item/Product master

DOCUMENTATION:
    https://online-help.sagex3.com/erp/12/en-us/Content/V7DEV/integration-guide_ws-overview.html
"""

import base64
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, date
from decimal import Decimal
import os

logger = logging.getLogger(__name__)


def to_float(val):
    """Convert value to float safely."""
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def to_str(val):
    """Convert value to string safely."""
    if val is None:
        return ""
    return str(val).strip()


class SageX3Reader:
    """
    Reads data from Sage X3 via SOAP Web Services.

    Usage:
        reader = SageX3Reader(
            base_url="http://server:8124",
            folder="SWIFT",
            username="admin",
            password="password"
        )
        reader.connect()
        invoices = reader.get_sales_invoices(from_date="2025-01-01")
    """

    def __init__(self, base_url=None, folder=None, username=None, password=None):
        """
        Initialize the X3 reader.

        Args:
            base_url: X3 Syracuse server URL (e.g., http://20.237.201.146:8124)
            folder: X3 folder/endpoint name (e.g., SWIFT, SEED)
            username: X3 web services username
            password: X3 web services password
        """
        # Load from environment or config if not provided
        self.base_url = (base_url or os.environ.get("X3_BASE_URL", "")).rstrip("/")
        self.folder = folder or os.environ.get("X3_FOLDER", "SWIFT")
        self.username = username or os.environ.get("X3_USERNAME", "")
        self.password = password or os.environ.get("X3_PASSWORD", "")

        self.session = None
        self._customer_cache = None
        self._item_cache = None

    @property
    def api_base(self):
        """Get the REST API base URL for this folder."""
        return f"{self.base_url}/api1/x3/erp/{self.folder}"

    @property
    def soap_url(self):
        """Get the SOAP service URL."""
        return f"{self.base_url}/soap-generic/syracuse/collaboration/syracuse/CAdxWebServiceXmlCC"

    def _get_auth_header(self):
        """Generate Basic Auth header."""
        credentials = f"{self.username}:{self.password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def connect(self):
        """
        Establish connection and test authentication.
        Returns True if successful, False otherwise.
        """
        try:
            import requests
            from requests.auth import HTTPBasicAuth
        except ImportError:
            logger.error("requests library not installed. Run: pip install requests")
            return False

        if not self.base_url or not self.username:
            logger.error("X3 connection details not configured")
            return False

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

        # Test connection with a simple REST request
        try:
            test_url = f"{self.api_base}/BPCUSTOMER?representation=BPCUSTOMER.$query&count=1"
            response = self.session.get(test_url, timeout=30)

            if response.status_code == 200:
                logger.info(f"Connected to Sage X3 folder: {self.folder}")
                return True
            elif response.status_code == 401:
                logger.error("X3 authentication failed - check username/password")
                return False
            elif response.status_code == 403:
                logger.error("X3 access denied - user may lack permissions")
                return False
            else:
                logger.error(f"X3 connection failed: HTTP {response.status_code}")
                logger.debug(f"Response: {response.text[:500]}")
                return False

        except Exception as e:
            logger.error(f"X3 connection error: {e}")
            return False

    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
            self.session = None

    # ----------------------------------------------------------------
    # SOAP WEB SERVICE METHODS
    # ----------------------------------------------------------------

    def _soap_query(self, publication, list_size=100):
        """
        Call SOAP query method to list records.

        Args:
            publication: Publication name (e.g., "XSIH")
            list_size: Number of records to return

        Returns:
            List of dicts with record data
        """
        if not self.session:
            if not self.connect():
                return []

        soap_body = f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:wss="http://www.adonix.com/WSS">
   <soapenv:Header/>
   <soapenv:Body>
      <wss:query>
         <callContext>
            <codeLang>ENG</codeLang>
            <poolAlias>{self.folder}</poolAlias>
            <poolId></poolId>
            <requestConfig>adxwss.trace.on=off&amp;adxwss.beautify=true</requestConfig>
         </callContext>
         <publicName>{publication}</publicName>
         <objectKeys></objectKeys>
         <listSize>{list_size}</listSize>
      </wss:query>
   </soapenv:Body>
</soapenv:Envelope>'''

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '""',
        }

        try:
            response = self.session.post(
                self.soap_url,
                data=soap_body.encode('utf-8'),
                headers=headers,
                timeout=120
            )

            if response.status_code == 200:
                return self._parse_soap_list_response(response.text)
            else:
                logger.error(f"SOAP query failed for {publication}: HTTP {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"SOAP query error for {publication}: {e}")
            return []

    def _soap_read(self, publication, key_name, key_value):
        """
        Call SOAP read method to get a single record with full details.

        Args:
            publication: Publication name (e.g., "XSIH")
            key_name: Key field name (e.g., "NUM")
            key_value: Key value (e.g., "ZAINV2512SWI03000680")

        Returns:
            Dict with full record data
        """
        if not self.session:
            if not self.connect():
                return {}

        soap_body = f'''<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:wss="http://www.adonix.com/WSS">
   <soapenv:Header/>
   <soapenv:Body>
      <wss:read>
         <callContext>
            <codeLang>ENG</codeLang>
            <poolAlias>{self.folder}</poolAlias>
            <poolId></poolId>
            <requestConfig>adxwss.trace.on=off&amp;adxwss.beautify=true</requestConfig>
         </callContext>
         <publicName>{publication}</publicName>
         <objectKeys>
            <CAdxParamKeyValue>
               <key>{key_name}</key>
               <value>{key_value}</value>
            </CAdxParamKeyValue>
         </objectKeys>
      </wss:read>
   </soapenv:Body>
</soapenv:Envelope>'''

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '""',
        }

        try:
            response = self.session.post(
                self.soap_url,
                data=soap_body.encode('utf-8'),
                headers=headers,
                timeout=60
            )

            if response.status_code == 200:
                return self._parse_soap_read_response(response.text)
            else:
                logger.error(f"SOAP read failed for {publication}/{key_value}: HTTP {response.status_code}")
                return {}

        except Exception as e:
            logger.error(f"SOAP read error for {publication}/{key_value}: {e}")
            return {}

    def _parse_soap_list_response(self, xml_text):
        """Parse SOAP query response to extract list of records."""
        records = []

        # Extract resultXml CDATA content
        match = re.search(r'<resultXml[^>]*><!\[CDATA\[(.+?)\]\]></resultXml>', xml_text, re.DOTALL)
        if not match:
            return records

        result_xml = match.group(1)

        # Parse each LIN element
        lin_matches = re.findall(r'<LIN NUM="(\d+)">(.*?)</LIN>', result_xml, re.DOTALL)
        for lin_num, lin_content in lin_matches:
            record = {}
            # Extract field values
            fld_matches = re.findall(r'<FLD NAME="([^"]+)"[^>]*>([^<]*)</FLD>', lin_content)
            for name, value in fld_matches:
                record[name] = value
            records.append(record)

        return records

    # Robust FLD parser: matches <FLD ...>value</FLD> with NAME attribute
    # anywhere in the attribute list (not just first). Also picks up MENULAB.
    _FLD_RE = re.compile(r'<FLD\b([^>]*?)>([^<]*)</FLD>')
    _FLD_NAME_RE = re.compile(r'\bNAME="([^"]+)"')
    _FLD_MENULAB_RE = re.compile(r'\bMENULAB="([^"]+)"')

    def _extract_flds(self, content):
        """Extract FLD fields from an XML chunk into {name: value}.

        Also stores MENULAB (human label for enum fields) under "_{name}_label".
        Handles attributes in any order.
        """
        out = {}
        for m in self._FLD_RE.finditer(content):
            attrs = m.group(1)
            value = m.group(2)
            name_m = self._FLD_NAME_RE.search(attrs)
            if not name_m:
                continue
            name = name_m.group(1)
            out[name] = value
            label_m = self._FLD_MENULAB_RE.search(attrs)
            if label_m:
                out[f"_{name}_label"] = label_m.group(1)
        return out

    def _parse_soap_read_response(self, xml_text):
        """Parse SOAP read response to extract full record with groups."""
        result = {}

        # Extract resultXml CDATA content
        match = re.search(r'<resultXml[^>]*><!\[CDATA\[(.+?)\]\]></resultXml>', xml_text, re.DOTALL)
        if not match:
            return result

        result_xml = match.group(1)

        # Parse each GRP element
        grp_matches = re.findall(r'<GRP ID="([^"]+)">(.*?)</GRP>', result_xml, re.DOTALL)
        for grp_id, grp_content in grp_matches:
            # Extract field values (attribute order agnostic + MENULAB capture)
            result.update(self._extract_flds(grp_content))

            # Extract list values (for line items)
            lst_matches = re.findall(r'<LST NAME="([^"]+)"[^>]*>(.*?)</LST>', grp_content, re.DOTALL)
            for lst_name, lst_content in lst_matches:
                items = re.findall(r'<ITM>([^<]*)</ITM>', lst_content)
                result[lst_name] = items

        # Parse TAB elements for line items (SIH4_1 contains invoice lines)
        # Use a more robust approach - find each TAB element by searching for start/end tags
        tab_pattern = re.compile(r'<TAB[^>]*ID="([^"]+)"[^>]*>([\s\S]*?)</TAB>')
        for tab_match in tab_pattern.finditer(result_xml):
            tab_id = tab_match.group(1)
            tab_content = tab_match.group(2)

            lines = []
            # Find each LIN element within this TAB
            lin_pattern = re.compile(r'<LIN NUM="(\d+)">([\s\S]*?)</LIN>')
            for lin_match in lin_pattern.finditer(tab_content):
                lin_num = lin_match.group(1)
                lin_content = lin_match.group(2)

                line = self._extract_flds(lin_content)
                if line:  # Only add if we got some fields
                    lines.append(line)

            if lines:
                result[f"_lines_{tab_id}"] = lines
                logger.debug(f"Parsed {len(lines)} lines from TAB {tab_id}")

        return result

    # ----------------------------------------------------------------
    # REST API METHODS (for resources that work via REST)
    # ----------------------------------------------------------------

    def _request(self, resource, params=None, method="GET"):
        """
        Make a REST API request to X3.

        Args:
            resource: Resource name (e.g., BPCUSTOMER)
            params: Query parameters dict
            method: HTTP method

        Returns:
            List of resource records or empty list on error
        """
        if not self.session:
            if not self.connect():
                return []

        url = f"{self.api_base}/{resource}"

        # Build query string manually to avoid encoding issues
        if params:
            query_parts = [f"{k}={v}" for k, v in params.items()]
            url = f"{url}?{'&'.join(query_parts)}"
        else:
            url = f"{url}?representation={resource}.$query"

        # Update headers for REST
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        try:
            response = self.session.request(method, url, headers=headers, timeout=60)

            if response.status_code == 200:
                data = response.json()
                return data.get("$resources", [])
            else:
                logger.error(f"X3 REST API error for {resource}: HTTP {response.status_code}")
                logger.debug(f"Response: {response.text[:500]}")
                return []

        except Exception as e:
            logger.error(f"X3 REST request failed for {resource}: {e}")
            return []

    def _get_single(self, resource, key):
        """
        Get a single resource by its key via REST.

        Args:
            resource: Resource name
            key: Primary key value (e.g., invoice number)

        Returns:
            Dict with resource data or empty dict
        """
        if not self.session:
            if not self.connect():
                return {}

        url = f"{self.api_base}/{resource}('{key}')"

        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            logger.error(f"X3 get single failed for {resource}/{key}: {e}")
            return {}

    # ----------------------------------------------------------------
    # CUSTOMERS
    # ----------------------------------------------------------------

    def _build_customer_cache(self):
        """Build customer lookup cache."""
        if self._customer_cache is not None:
            return self._customer_cache

        self._customer_cache = {}

        # Fetch all customers via REST with proper URL format
        # Key X3 fields: BPCNUM (ID), BPCNAM (Name), CRN (Tax ID/TIN)
        url = f"{self.api_base}/BPCUSTOMER?representation=BPCUSTOMER.$query&count=5000"

        try:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            # Increased timeout to 180 seconds for large customer lists
            response = self.session.get(url, headers=headers, timeout=180)

            if response.status_code == 200:
                data = response.json()
                customers = data.get("$resources", [])

                for cust in customers:
                    cust_id = to_str(cust.get("BPCNUM"))
                    if cust_id:
                        self._customer_cache[cust_id] = {
                            "customer_id": cust_id,
                            "name": to_str(cust.get("BPCNAM")),
                            "tin": to_str(cust.get("CRN")),  # Tax Registration Number
                            "email": to_str(cust.get("WEB", "")),  # or EMAIL field
                            "phone": to_str(cust.get("TEL", "")),
                            "address": to_str(cust.get("BPCADD", "")),
                            "city": to_str(cust.get("CTY", "")),
                            "country": to_str(cust.get("CRY", "NG")),
                            "postal_code": to_str(cust.get("POSCOD", "")),
                        }
            else:
                logger.warning(f"Customer cache fetch failed: HTTP {response.status_code}")
        except Exception as e:
            logger.warning(f"Could not build customer cache: {e}")
            # Continue without customer cache - invoice data will still work

        logger.info(f"Built customer cache: {len(self._customer_cache)} customers")
        return self._customer_cache

    def get_customers(self):
        """Get all customers as dict keyed by customer ID."""
        return self._build_customer_cache()

    def get_customer(self, customer_id):
        """Get a single customer by ID."""
        return self.get_customers().get(customer_id, {})

    # ----------------------------------------------------------------
    # ITEMS
    # ----------------------------------------------------------------

    def _build_item_cache(self):
        """Build item/product lookup cache."""
        if self._item_cache is not None:
            return self._item_cache

        self._item_cache = {}

        # Fetch items - ITMMASTER is the item master table
        # Key fields: ITMREF (Item code), ITMDES1 (Description)
        items = self._request("ITMMASTER", {"count": 10000})

        for item in items:
            item_code = to_str(item.get("ITMREF"))
            if item_code:
                self._item_cache[item_code] = {
                    "item_id": item_code,
                    "description": to_str(item.get("ITMDES1") or item.get("ITMDES")),
                    "unit": to_str(item.get("SAU", "EA")),  # Sales unit
                    "price": to_float(item.get("BASPRI", 0)),  # Base price
                }

        logger.info(f"Built item cache: {len(self._item_cache)} items")
        return self._item_cache

    def get_items(self):
        """Get all items as dict keyed by item code."""
        return self._build_item_cache()

    # ----------------------------------------------------------------
    # SALES INVOICES (via SOAP - XSIH publication)
    # ----------------------------------------------------------------

    def get_sales_invoices(self, from_date=None, to_date=None, limit=None, allowed_company_codes=None):
        """
        Fetch sales invoices from X3 via SOAP (XSIH publication).

        X3 Invoice Structure (XSIH):
        - Header: NUM, INVDAT, BPCINV, BPINAM, CUR, INVNOT, INVATI
        - Lines: SIH4_1 group with ITMREF, ITMDES1, QTY, NETPRI, AMTNOTLIN

        Args:
            from_date: Start date (YYYY-MM-DD) - optional filter
            to_date: End date (YYYY-MM-DD) - optional filter
            limit: Maximum number of invoices
            allowed_company_codes: If provided (list/set), only keep invoices
                whose SALFCY is in this list.

        Returns:
            Dict of invoices keyed by invoice number
        """
        allowed = set(allowed_company_codes) if allowed_company_codes else None
        # First, get list of invoice numbers via SOAP query
        invoice_list = self._soap_query("XSIH", list_size=limit or 1000)

        if not invoice_list:
            logger.warning("No invoices found in X3 via SOAP")
            return {}

        logger.info(f"Found {len(invoice_list)} invoices from X3 SOAP query")

        # Build customer cache for additional lookups
        customers = self._build_customer_cache()

        invoices = {}
        for inv_summary in invoice_list:
            inv_num = to_str(inv_summary.get("NUM"))
            if not inv_num:
                continue

            # Parse date from X3 format (DD/MM/YYYY)
            inv_date_raw = to_str(inv_summary.get("INVDAT", ""))
            inv_date = self._parse_x3_date(inv_date_raw)

            # Apply date filters if provided
            if from_date and inv_date < from_date:
                continue
            if to_date and inv_date > to_date:
                continue

            # Get full invoice details via SOAP read
            full_invoice = self._soap_read("XSIH", "NUM", inv_num)
            if not full_invoice:
                continue

            # Only bring in POSTED invoices (INVSTA -> MENULAB = "Posted")
            if not self._is_posted(full_invoice):
                continue

            # Company/site filter (SALFCY)
            salfcy = to_str(full_invoice.get("SALFCY", "")).strip()
            if allowed is not None and salfcy not in allowed:
                continue

            # Get customer info
            cust_id = to_str(full_invoice.get("BPCINV") or inv_summary.get("BPCINV"))
            cust = customers.get(cust_id, {})

            # Get amounts
            subtotal = to_float(full_invoice.get("INVNOT", 0))
            total_amount = to_float(full_invoice.get("INVATI", 0))
            vat_amount = total_amount - subtotal

            # Determine invoice type - check document prefix and amount
            # ZACCN = Credit Note, ZAINV = Invoice
            if inv_num.startswith("ZACCN") or total_amount < 0:
                inv_type = "Credit Note"
            else:
                inv_type = "Invoice"

            # Get description from DES list
            description = ""
            if "DES" in full_invoice and isinstance(full_invoice["DES"], list):
                description = " ".join([d for d in full_invoice["DES"] if d])

            invoices[inv_num] = {
                "invoice_number": inv_num,
                "date": inv_date,
                "customer_id": cust_id,
                "customer_name": to_str(full_invoice.get("BPINAM")) or cust.get("name", cust_id),
                "customer_email": cust.get("email", ""),
                "customer_phone": cust.get("phone", ""),
                "customer_address": cust.get("address", ""),
                "customer_city": cust.get("city", ""),
                "customer_tin": cust.get("tin", ""),
                "customer_country": cust.get("country", "NG"),
                "company_code": to_str(full_invoice.get("SALFCY", "")),
                "company_name": to_str(full_invoice.get("ZSALFCY", "")) or to_str(full_invoice.get("SALFCY", "")),
                "status_code": to_str(full_invoice.get("INVSTA", "")),
                "status_label": to_str(full_invoice.get("_INVSTA_label", "")),
                "invoice_type": inv_type,
                "currency": to_str(full_invoice.get("CUR", "NGN")),
                "subtotal": abs(subtotal),
                "vat_amount": abs(vat_amount),
                "total_amount": abs(total_amount),
                "description": description,
                "lines": [],
            }

            # Extract line items from the TAB groups
            self._extract_invoice_lines(full_invoice, invoices[inv_num])

        # Filter out invoices with no lines
        invoices = {k: v for k, v in invoices.items() if v.get("lines")}

        logger.info(f"Processed {len(invoices)} invoices with line items")
        return invoices

    def _is_posted(self, full_record):
        """Return True if an X3 sales invoice/credit note is in the Posted state.

        Prefers the MENULAB label on INVSTA (e.g. "Posted") since the numeric
        value varies across X3 installations (commonly 2 or 3 for Posted).
        Falls back to accepting numeric 2 or 3, or '' if the field is missing.
        """
        label = to_str(full_record.get("_INVSTA_label", "")).strip().lower()
        if label:
            return label == "posted"
        val = to_str(full_record.get("INVSTA", "")).strip()
        if val in ("2", "3"):
            return True
        # If the publication doesn't expose INVSTA at all, don't block everything.
        return val == ""

    def _parse_x3_date(self, date_str):
        """Parse X3 date format (DD/MM/YYYY or YYYYMMDD) to YYYY-MM-DD."""
        if not date_str:
            return ""

        date_str = date_str.strip()

        # Try DD/MM/YYYY format
        if "/" in date_str:
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    day, month, year = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except:
                pass

        # Try YYYYMMDD format
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        return date_str[:10]

    def _extract_invoice_lines(self, full_invoice, invoice_dict):
        """Extract line items from SOAP read response."""
        # Look specifically for SIH4_1 which contains the actual invoice lines
        # Other TABs (SIH2_4, SIH2_5, SIHV_2) contain other data like taxes, payments, etc.
        lines_tab = full_invoice.get("_lines_SIH4_1", [])

        # Get VAT rate from SIHV_2 TAB (VAT breakdown) - contains actual XVRAT value
        vat_tab = full_invoice.get("_lines_SIHV_2", [])
        invoice_vat_rate = 0.0
        if vat_tab:
            # Use the first VAT line's rate (XVRAT field)
            invoice_vat_rate = to_float(vat_tab[0].get("XVRAT", 0))

        for line in lines_tab:
            qty = to_float(line.get("QTY", 0))
            unit_price = to_float(line.get("NETPRI", 0))
            # Calculate line total if not available (QTY * NETPRI)
            line_total = to_float(line.get("AMTNOTLIN", 0))
            if line_total == 0 and qty != 0 and unit_price != 0:
                line_total = qty * unit_price

            # Get discount (sum of all discount fields)
            discount = (to_float(line.get("DISCRGVAL1", 0)) +
                       to_float(line.get("DISCRGVAL2", 0)) +
                       to_float(line.get("DISCRGVAL3", 0)))

            # Get VAT info - first check line-level VACITM1, then use invoice-level rate
            vat_code = to_str(line.get("VACITM1", ""))
            if vat_code == "ZERO":
                vat_rate = 0.0
            elif vat_code and "7" in vat_code:
                # VAT codes containing "7" are typically 7.5% (e.g., VAT75, VFOE7)
                vat_rate = 7.5
            elif invoice_vat_rate > 0:
                vat_rate = invoice_vat_rate
            else:
                vat_rate = 0.0  # Default to 0 if no VAT info found

            # Skip empty lines
            if qty == 0 and line_total == 0:
                continue

            # Skip lines with no item reference (header/summary lines)
            item_code = to_str(line.get("ITMREF", ""))
            if not item_code:
                continue

            invoice_dict["lines"].append({
                "item_code": item_code,
                "description": to_str(line.get("ITMDES1") or line.get("ITMDES", "Service")),
                "quantity": abs(qty) if qty != 0 else 1,
                "unit_price": abs(unit_price) if unit_price != 0 else abs(line_total),
                "discount": abs(discount),
                "tax_rate": vat_rate,
                "line_total": abs(line_total),
                "unit": to_str(line.get("SAU") or line.get("STU", "EA")),
                "vat_code": vat_code,  # Include VAT code for reference
            })

        # If no lines found in SIH4_1, create a single line from totals (fallback)
        if not invoice_dict["lines"] and invoice_dict["total_amount"] > 0:
            invoice_dict["lines"].append({
                "item_code": "SERVICE",
                "description": invoice_dict.get("description") or "Service/Product",
                "quantity": 1,
                "unit_price": invoice_dict["subtotal"],
                "discount": 0,
                "tax_rate": 7.5 if invoice_dict["vat_amount"] > 0 else 0,
                "line_total": invoice_dict["subtotal"],
                "unit": "EA",
            })

    def get_invoice_by_number(self, invoice_number):
        """Get a single invoice by its number via SOAP."""
        # Read the full invoice directly
        full_invoice = self._soap_read("XSIH", "NUM", invoice_number)
        if not full_invoice:
            return None

        customers = self._build_customer_cache()
        cust_id = to_str(full_invoice.get("BPCINV"))
        cust = customers.get(cust_id, {})

        inv_date_raw = to_str(full_invoice.get("INVDAT", ""))
        inv_date = self._parse_x3_date(inv_date_raw)

        subtotal = to_float(full_invoice.get("INVNOT", 0))
        total_amount = to_float(full_invoice.get("INVATI", 0))
        vat_amount = total_amount - subtotal

        description = ""
        if "DES" in full_invoice and isinstance(full_invoice["DES"], list):
            description = " ".join([d for d in full_invoice["DES"] if d])

        invoice = {
            "invoice_number": invoice_number,
            "date": inv_date,
            "customer_id": cust_id,
            "customer_name": to_str(full_invoice.get("BPINAM")) or cust.get("name", cust_id),
            "customer_email": cust.get("email", ""),
            "customer_phone": cust.get("phone", ""),
            "customer_address": cust.get("address", ""),
            "customer_city": cust.get("city", ""),
            "customer_tin": cust.get("tin", ""),
            "customer_country": cust.get("country", "NG"),
            "invoice_type": "Credit Note" if total_amount < 0 else "Invoice",
            "currency": to_str(full_invoice.get("CUR", "NGN")),
            "subtotal": abs(subtotal),
            "vat_amount": abs(vat_amount),
            "total_amount": abs(total_amount),
            "description": description,
            "lines": [],
        }

        self._extract_invoice_lines(full_invoice, invoice)
        return invoice

    # ----------------------------------------------------------------
    # CREDIT NOTES (via SOAP - XCRN publication)
    # ----------------------------------------------------------------

    def get_credit_notes(self, from_date=None, to_date=None, limit=None, allowed_company_codes=None):
        """
        Fetch credit notes from X3 via SOAP (XCRN publication).

        X3 Credit Note Structure (XCRN):
        - Header: NUM, INVDAT, BPCINV, BPINAM, CUR, INVNOT, INVATI, SIHORINUM
        - Lines: SIH4_1 group with ITMREF, ITMDES1, QTY, NETPRI, AMTNOTLIN

        Args:
            from_date: Start date (YYYY-MM-DD) - optional filter
            to_date: End date (YYYY-MM-DD) - optional filter
            limit: Maximum number of credit notes
            allowed_company_codes: If provided, only keep credit notes whose
                SALFCY is in this list.

        Returns:
            Dict of credit notes keyed by credit note number
        """
        allowed = set(allowed_company_codes) if allowed_company_codes else None
        # Get list of credit note numbers via SOAP query
        crn_list = self._soap_query("XCRN", list_size=limit or 1000)

        if not crn_list:
            logger.info("No credit notes found in X3 via SOAP (XCRN)")
            return {}

        logger.info(f"Found {len(crn_list)} credit notes from X3 SOAP query")

        # Build customer cache for additional lookups
        customers = self._build_customer_cache()

        credit_notes = {}
        for crn_summary in crn_list:
            crn_num = to_str(crn_summary.get("NUM"))
            if not crn_num:
                continue

            # Parse date from X3 format (DD/MM/YYYY)
            crn_date_raw = to_str(crn_summary.get("INVDAT", ""))
            crn_date = self._parse_x3_date(crn_date_raw)

            # Apply date filters if provided
            if from_date and crn_date < from_date:
                continue
            if to_date and crn_date > to_date:
                continue

            # Get full credit note details via SOAP read
            full_crn = self._soap_read("XCRN", "NUM", crn_num)
            if not full_crn:
                continue

            # Only bring in POSTED credit notes
            if not self._is_posted(full_crn):
                continue

            # Company/site filter (SALFCY)
            salfcy = to_str(full_crn.get("SALFCY", "")).strip()
            if allowed is not None and salfcy not in allowed:
                continue

            # Get customer info
            cust_id = to_str(full_crn.get("BPCINV") or crn_summary.get("BPCINV"))
            cust = customers.get(cust_id, {})

            # Get amounts
            subtotal = to_float(full_crn.get("INVNOT", 0))
            total_amount = to_float(full_crn.get("INVATI", 0))
            vat_amount = total_amount - subtotal

            # Get original invoice reference from multiple possible sources
            original_invoice = to_str(full_crn.get("SIHORINUM", ""))

            # Check other common X3 field names for linked invoice reference
            if not original_invoice:
                original_invoice = to_str(full_crn.get("SRCNUM", ""))
            if not original_invoice:
                original_invoice = to_str(full_crn.get("ORISIH", ""))
            if not original_invoice:
                original_invoice = to_str(full_crn.get("ORIINV", ""))
            if not original_invoice:
                original_invoice = to_str(full_crn.get("BESSION", ""))

            # Check in linked documents TAB (SIH6 or similar)
            linked_tab = full_crn.get("_lines_SIH6_1", []) or full_crn.get("_lines_SIH6", [])
            if not original_invoice and linked_tab:
                for linked_doc in linked_tab:
                    # Check if this is an invoice link (SRCTYP = "INV" or similar)
                    src_type = to_str(linked_doc.get("SRCTYP", "")).upper()
                    if src_type in ("INV", "SIH", "INVOICE", ""):
                        original_invoice = to_str(linked_doc.get("SRCNUM", ""))
                        if original_invoice:
                            break

            # Also check in _lines_SIH5 which may contain original references
            ref_tab = full_crn.get("_lines_SIH5_1", []) or full_crn.get("_lines_SIH5", [])
            if not original_invoice and ref_tab:
                for ref_doc in ref_tab:
                    ref_num = to_str(ref_doc.get("BESSION", "")) or to_str(ref_doc.get("NUM", ""))
                    if ref_num and ref_num.startswith("ZAINV"):
                        original_invoice = ref_num
                        break

            # Extract from fields that may contain the linked document info
            # Some X3 installations use NUMORI, ORIGINE, or custom fields
            if not original_invoice:
                original_invoice = to_str(full_crn.get("NUMORI", ""))
            if not original_invoice:
                original_invoice = to_str(full_crn.get("ORIGINE", ""))

            # Get description from DES list
            description = ""
            if "DES" in full_crn and isinstance(full_crn["DES"], list):
                description = " ".join([d for d in full_crn["DES"] if d])
            if not description and original_invoice:
                description = f"Credit Note for Invoice {original_invoice}"

            credit_notes[crn_num] = {
                "invoice_number": crn_num,
                "date": crn_date,
                "customer_id": cust_id,
                "customer_name": to_str(full_crn.get("BPINAM")) or cust.get("name", cust_id),
                "customer_email": cust.get("email", ""),
                "customer_phone": cust.get("phone", ""),
                "customer_address": cust.get("address", ""),
                "customer_city": cust.get("city", ""),
                "customer_tin": cust.get("tin", ""),
                "customer_country": cust.get("country", "NG"),
                "company_code": to_str(full_crn.get("SALFCY", "")),
                "company_name": to_str(full_crn.get("ZSALFCY", "")) or to_str(full_crn.get("SALFCY", "")),
                "status_code": to_str(full_crn.get("INVSTA", "")),
                "status_label": to_str(full_crn.get("_INVSTA_label", "")),
                "invoice_type": "Credit Note",
                "original_invoice": original_invoice,
                "currency": to_str(full_crn.get("CUR", "NGN")),
                "subtotal": abs(subtotal),
                "vat_amount": abs(vat_amount),
                "total_amount": abs(total_amount),
                "description": description,
                "lines": [],
            }

            # Extract line items (same structure as invoices)
            self._extract_invoice_lines(full_crn, credit_notes[crn_num])

        # Filter out credit notes with no lines
        credit_notes = {k: v for k, v in credit_notes.items() if v.get("lines")}

        logger.info(f"Processed {len(credit_notes)} credit notes with line items")
        return credit_notes

    # ----------------------------------------------------------------
    # XBIC PUBLICATION (Combined Invoices and Credit Notes)
    # ----------------------------------------------------------------

    def get_xbic_documents(self, from_date=None, to_date=None, limit=None, allowed_company_codes=None):
        """
        Fetch documents from X3 via SOAP (XBIC publication).
        XBIC contains both invoices and credit notes in a single publication.

        X3 XBIC Structure:
        - Header: NUM, SIVTYP, ACCDAT, BPR, BPRNAM, CUR, AMTNOT, AMTATI
        - Lines: BIC3_1 group with ACC1, AMTNOTLIN, AMTVAT, AMTATILIN, DES, QTY

        Args:
            from_date: Start date (YYYY-MM-DD) - optional filter
            to_date: End date (YYYY-MM-DD) - optional filter
            limit: Maximum number of documents
            allowed_company_codes: If provided, only keep documents whose
                SALFCY/FCY is in this list.

        Returns:
            Dict of documents keyed by document number
        """
        allowed = set(allowed_company_codes) if allowed_company_codes else None
        # Get list of documents via SOAP query
        doc_list = self._soap_query("XBIC", list_size=limit or 1000)

        if not doc_list:
            logger.info("No documents found in X3 via SOAP (XBIC)")
            return {}

        logger.info(f"Found {len(doc_list)} documents from X3 XBIC query")

        # Build customer cache for additional lookups
        customers = self._build_customer_cache()

        documents = {}
        for doc_summary in doc_list:
            doc_num = to_str(doc_summary.get("NUM"))
            if not doc_num:
                continue

            # Parse date from X3 format (DD/MM/YYYY)
            doc_date_raw = to_str(doc_summary.get("ACCDAT", ""))
            doc_date = self._parse_x3_date(doc_date_raw)

            # Apply date filters if provided
            if from_date and doc_date < from_date:
                continue
            if to_date and doc_date > to_date:
                continue

            # Get full document details via SOAP read
            full_doc = self._soap_read("XBIC", "NUM", doc_num)
            if not full_doc:
                continue

            # Only bring in POSTED documents
            if not self._is_posted(full_doc):
                continue

            # Company/site filter (SALFCY / FCY)
            salfcy = to_str(full_doc.get("SALFCY", "") or full_doc.get("FCY", "")).strip()
            if allowed is not None and salfcy not in allowed:
                continue

            # Get customer info
            cust_id = to_str(full_doc.get("BPR") or doc_summary.get("BPR"))
            cust = customers.get(cust_id, {})

            # Get amounts
            subtotal = to_float(full_doc.get("AMTNOT") or full_doc.get("TOTNOT", 0))
            total_amount = to_float(full_doc.get("AMTATI") or full_doc.get("TOTATI", 0))
            vat_amount = total_amount - subtotal

            # Determine document type from SIVTYP field
            sivtyp = to_str(full_doc.get("SIVTYP", ""))
            if sivtyp == "ZACRN" or doc_num.startswith("ZACCN"):
                doc_type = "Credit Note"
            else:
                doc_type = "Invoice"

            # Get description from DES list
            description = ""
            if "DES" in full_doc and isinstance(full_doc["DES"], list):
                description = " ".join([d for d in full_doc["DES"] if d])

            # Get original invoice reference for credit notes
            original_invoice = to_str(full_doc.get("INVNUM", ""))

            documents[doc_num] = {
                "invoice_number": doc_num,
                "date": doc_date,
                "customer_id": cust_id,
                "customer_name": to_str(full_doc.get("BPRNAM")) or cust.get("name", cust_id),
                "customer_email": cust.get("email", ""),
                "customer_phone": cust.get("phone", ""),
                "customer_address": cust.get("address", ""),
                "customer_city": cust.get("city", ""),
                "customer_tin": cust.get("tin", ""),
                "customer_country": cust.get("country", "NG"),
                "company_code": to_str(full_doc.get("SALFCY", "") or full_doc.get("FCY", "")),
                "company_name": to_str(full_doc.get("ZSALFCY", "") or full_doc.get("ZFCY", "")) or to_str(full_doc.get("SALFCY", "") or full_doc.get("FCY", "")),
                "status_code": to_str(full_doc.get("INVSTA", "")),
                "status_label": to_str(full_doc.get("_INVSTA_label", "")),
                "invoice_type": doc_type,
                "original_invoice": original_invoice,
                "currency": to_str(full_doc.get("CUR", "NGN")),
                "subtotal": abs(subtotal),
                "vat_amount": abs(vat_amount),
                "total_amount": abs(total_amount),
                "description": description,
                "lines": [],
                "source": "XBIC",  # Track which publication this came from
            }

            # Extract line items from XBIC structure
            self._extract_xbic_lines(full_doc, documents[doc_num])

        # Filter out documents with no lines
        documents = {k: v for k, v in documents.items() if v.get("lines")}

        logger.info(f"Processed {len(documents)} XBIC documents with line items")
        return documents

    def _extract_xbic_lines(self, full_doc, doc_dict):
        """Extract line items from XBIC SOAP read response."""
        # XBIC uses BIC3_1 for line items
        lines_tab = full_doc.get("_lines_BIC3_1", [])

        for line in lines_tab:
            qty = to_float(line.get("QTY", 1)) or 1
            line_total = to_float(line.get("AMTNOTLIN", 0))
            vat_amount = to_float(line.get("AMTVAT", 0))
            unit_price = line_total / qty if qty != 0 else line_total

            # Calculate VAT rate
            tax_rate = 0.0
            if line_total > 0 and vat_amount > 0:
                tax_rate = round((vat_amount / line_total) * 100, 2)

            description = to_str(line.get("DES", ""))
            if not description:
                description = "Product/Service"

            doc_dict["lines"].append({
                "item_code": to_str(line.get("ACC1", "SERVICE")),
                "description": description,
                "quantity": qty,
                "unit_price": unit_price,
                "discount": 0,
                "tax_rate": tax_rate,
                "line_total": line_total,
                "unit": "EA",
            })

        # If no lines found, create a single line from totals (fallback)
        if not doc_dict["lines"] and doc_dict["total_amount"] > 0:
            doc_dict["lines"].append({
                "item_code": "SERVICE",
                "description": doc_dict.get("description") or "Service/Product",
                "quantity": 1,
                "unit_price": doc_dict["subtotal"],
                "discount": 0,
                "tax_rate": 7.5 if doc_dict["vat_amount"] > 0 else 0,
                "line_total": doc_dict["subtotal"],
                "unit": "EA",
            })

    def get_all_documents(self, from_date=None, to_date=None, limit=None, sources=None, allowed_company_codes=None):
        """
        Fetch documents from X3 using multiple publications.

        Args:
            from_date: Start date filter
            to_date: End date filter
            limit: Maximum records per publication
            sources: List of publications to use. Default: ["XSIH", "XCRN", "XBIC"]
                     Options: "XSIH" (invoices), "XCRN" (credit notes), "XBIC" (both)
            allowed_company_codes: List/set of SALFCY codes to keep. None = all.

        Returns:
            Dict of all documents keyed by document number
        """
        if sources is None:
            sources = ["XSIH", "XCRN", "XBIC"]

        all_docs = {}

        # Get from XSIH (Sales Invoices)
        if "XSIH" in sources:
            try:
                invoices = self.get_sales_invoices(from_date=from_date, to_date=to_date, limit=limit, allowed_company_codes=allowed_company_codes)
                for inv_num, inv in invoices.items():
                    inv["source"] = "XSIH"
                    all_docs[inv_num] = inv
                logger.info(f"XSIH: {len(invoices)} invoices")
            except Exception as e:
                logger.warning(f"XSIH fetch failed: {e}")

        # Get from XCRN (Credit Notes)
        if "XCRN" in sources:
            try:
                credit_notes = self.get_credit_notes(from_date=from_date, to_date=to_date, limit=limit, allowed_company_codes=allowed_company_codes)
                for crn_num, crn in credit_notes.items():
                    crn["source"] = "XCRN"
                    all_docs[crn_num] = crn
                logger.info(f"XCRN: {len(credit_notes)} credit notes")
            except Exception as e:
                logger.warning(f"XCRN fetch failed: {e}")

        # Get from XBIC (Combined)
        if "XBIC" in sources:
            try:
                xbic_docs = self.get_xbic_documents(from_date=from_date, to_date=to_date, limit=limit, allowed_company_codes=allowed_company_codes)
                for doc_num, doc in xbic_docs.items():
                    # Only add if not already present (XSIH/XCRN take precedence)
                    if doc_num not in all_docs:
                        doc["source"] = "XBIC"
                        all_docs[doc_num] = doc
                logger.info(f"XBIC: {len(xbic_docs)} documents (new: {len([d for d in xbic_docs if d not in all_docs])})")
            except Exception as e:
                logger.warning(f"XBIC fetch failed: {e}")

        logger.info(f"Total documents from all sources: {len(all_docs)}")
        return all_docs

    # ----------------------------------------------------------------
    # COMPANY INFO
    # ----------------------------------------------------------------

    def get_company_info(self):
        """
        Get company/folder information.
        X3 stores this in COMPANY or similar resource.
        """
        companies = self._request("COMPANY", {"count": 1})
        if companies:
            comp = companies[0]
            return {
                "name": to_str(comp.get("CPYNAM")),
                "tin": to_str(comp.get("CRN")),
                "address": to_str(comp.get("CPYADD")),
                "city": to_str(comp.get("CTY")),
                "country": to_str(comp.get("CRY", "NG")),
            }
        return {}

    # ----------------------------------------------------------------
    # DISCOVERY/DEBUG
    # ----------------------------------------------------------------

    def test_endpoints(self):
        """Test various X3 API endpoints to discover what's available."""
        endpoints = [
            "SINVOICE",      # Sales invoices
            "SINVOICED",     # Sales invoice lines
            "BPCUSTOMER",    # Customers
            "ITMMASTER",     # Items
            "COMPANY",       # Company info
            "TABVAT",        # VAT rates
            "SORDER",        # Sales orders
            "SDELIVERY",     # Deliveries
        ]

        results = {}
        for ep in endpoints:
            try:
                data = self._request(ep, {"count": 1})
                results[ep] = {
                    "available": True,
                    "sample_fields": list(data[0].keys()) if data else [],
                    "sample_count": len(data),
                }
            except Exception as e:
                results[ep] = {"available": False, "error": str(e)}

        return results


# ============================================================
# ALTERNATIVE: DIRECT SQL SERVER CONNECTION
# ============================================================

class SageX3SQLReader:
    """
    Alternative reader that connects directly to X3's SQL Server database.
    Use this if REST web services are not available or too slow.

    WARNING: Direct database access bypasses X3 business logic.
    Use with caution and read-only queries.

    X3 Database Structure:
    - Each folder has its own schema (e.g., SWIFT.SINVOICE)
    - Field names have _0 suffix (e.g., NUM_0, ACCDAT_0)
    """

    def __init__(self, server=None, database=None, username=None, password=None, folder=None):
        """
        Initialize SQL connection.

        Args:
            server: SQL Server hostname/IP
            database: Database name (usually same as X3 solution)
            username: SQL Server username
            password: SQL Server password
            folder: X3 folder/schema name (e.g., SWIFT)
        """
        self.server = server or os.environ.get("X3_SQL_SERVER", "")
        self.database = database or os.environ.get("X3_SQL_DATABASE", "x3")
        self.username = username or os.environ.get("X3_SQL_USER", "")
        self.password = password or os.environ.get("X3_SQL_PASSWORD", "")
        self.folder = folder or os.environ.get("X3_FOLDER", "SWIFT")

        self.conn = None

    def connect(self):
        """Establish SQL Server connection."""
        try:
            import pyodbc
        except ImportError:
            logger.error("pyodbc not installed. Run: pip install pyodbc")
            return False

        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "TrustServerCertificate=yes;"
            )
            self.conn = pyodbc.connect(conn_str)
            logger.info(f"Connected to X3 SQL Server: {self.server}/{self.database}")
            return True
        except Exception as e:
            logger.error(f"SQL connection failed: {e}")
            return False

    def close(self):
        """Close the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_sales_invoices(self, from_date=None, to_date=None, limit=100):
        """
        Fetch invoices directly from SQL Server.

        X3 table names use the folder as schema.
        Field names have _0 suffix.
        """
        if not self.conn and not self.connect():
            return {}

        cursor = self.conn.cursor()

        # Query invoice headers with customer join
        query = f"""
        SELECT TOP {limit}
            h.NUM_0 as invoice_number,
            h.ACCDAT_0 as invoice_date,
            h.BPR_0 as customer_id,
            h.BPRNAM_0 as customer_name,
            c.CRN_0 as customer_tin,
            c.WEB_0 as customer_email,
            c.TEL_0 as customer_phone,
            h.AMTATI_0 as total_amount,
            h.AMTNOT_0 as subtotal,
            h.CUR_0 as currency
        FROM {self.folder}.SINVOICE h
        LEFT JOIN {self.folder}.BPCUSTOMER c ON h.BPR_0 = c.BPCNUM_0
        WHERE 1=1
        """

        params = []
        if from_date:
            query += " AND h.ACCDAT_0 >= ?"
            params.append(from_date)
        if to_date:
            query += " AND h.ACCDAT_0 <= ?"
            params.append(to_date)

        query += " ORDER BY h.ACCDAT_0 DESC"

        cursor.execute(query, params)
        columns = [c[0] for c in cursor.description]

        invoices = {}
        for row in cursor.fetchall():
            data = dict(zip(columns, row))
            inv_num = to_str(data["invoice_number"])

            invoices[inv_num] = {
                "invoice_number": inv_num,
                "date": str(data["invoice_date"])[:10] if data["invoice_date"] else "",
                "customer_id": to_str(data["customer_id"]),
                "customer_name": to_str(data["customer_name"]),
                "customer_tin": to_str(data.get("customer_tin", "")),
                "customer_email": to_str(data.get("customer_email", "")),
                "customer_phone": to_str(data.get("customer_phone", "")),
                "total_amount": to_float(data["total_amount"]),
                "subtotal": to_float(data["subtotal"]),
                "currency": to_str(data.get("currency", "NGN")),
                "lines": [],
            }

        # Fetch lines
        if invoices:
            self._fetch_lines_sql(cursor, invoices)

        return invoices

    def _fetch_lines_sql(self, cursor, invoices):
        """Fetch line items via SQL."""
        inv_nums = list(invoices.keys())
        placeholders = ",".join(["?" for _ in inv_nums])

        query = f"""
        SELECT
            NUM_0 as invoice_number,
            ITMREF_0 as item_code,
            ITMDES_0 as description,
            QTY_0 as quantity,
            NETPRI_0 as unit_price,
            VATRAT_0 as tax_rate,
            AMTNOT_0 as line_total
        FROM {self.folder}.SINVOICED
        WHERE NUM_0 IN ({placeholders})
        ORDER BY NUM_0, SIDLIN_0
        """

        cursor.execute(query, inv_nums)

        for row in cursor.fetchall():
            inv_num = to_str(row[0])
            if inv_num in invoices:
                invoices[inv_num]["lines"].append({
                    "item_code": to_str(row[1]),
                    "description": to_str(row[2]) or "Service",
                    "quantity": abs(to_float(row[3])) or 1,
                    "unit_price": abs(to_float(row[4])),
                    "tax_rate": to_float(row[5]),
                    "line_total": abs(to_float(row[6])),
                })


# ============================================================
# DISCOVERY / TESTING
# ============================================================

def discover_x3_database():
    """
    Discovery tool to explore X3 API structure.
    """
    print("\n" + "=" * 60)
    print("  SAGE X3 API DISCOVERY")
    print("=" * 60)

    reader = SageX3Reader()

    if not reader.connect():
        print("\n[FAIL] Could not connect to X3")
        print("Check your environment variables:")
        print("  X3_BASE_URL, X3_FOLDER, X3_USERNAME, X3_PASSWORD")
        return

    print(f"\n[OK] Connected to {reader.base_url}")
    print(f"    Folder: {reader.folder}")

    print("\n--- Testing Endpoints ---")
    results = reader.test_endpoints()
    for ep, info in results.items():
        status = "[OK]" if info.get("available") else "[--]"
        fields = ", ".join(info.get("sample_fields", [])[:5])
        print(f"  {status} {ep}: {fields}...")

    print("\n--- Sample Customers ---")
    customers = reader.get_customers()
    for i, (cid, c) in enumerate(list(customers.items())[:5]):
        print(f"  {cid}: {c['name']} | TIN: {c['tin']} | {c['city']}")
    print(f"  ... {len(customers)} total customers")

    print("\n--- Sample Invoices ---")
    invoices = reader.get_sales_invoices(limit=5)
    for inv_num, inv in invoices.items():
        print(f"  {inv_num}: {inv['date']} | {inv['customer_name']}")
        print(f"    Amount: {inv['currency']} {inv['total_amount']:,.2f} | Lines: {len(inv['lines'])}")
        for line in inv['lines'][:2]:
            print(f"      -> {line['item_code']}: {line['description'][:30]} | "
                  f"Qty: {line['quantity']} x {line['unit_price']:,.2f}")

    reader.close()
    print(f"\n[OK] Discovery complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    discover_x3_database()
