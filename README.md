# Nigeria FIRS E-Invoicing Integration (Sage X3)

Flask web application that integrates with Sage X3 ERP to sync sales invoices and credit notes, then submits them to FIRS (Federal Inland Revenue Service) via the Cryptware Systems API.

## Features

- Sync invoices and credit notes from Sage X3 via SOAP/REST API
- Submit documents to FIRS e-invoicing portal
- Generate PDF invoices with IRN and QR codes
- Support for credit notes with cancel_references
- Web dashboard for document management

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Credentials

```bash
cp config_x3.example.py config_x3.py
```

Edit `config_x3.py` with your:
- Sage X3 connection details (URL, folder, username, password)
- FIRS API key (test or production)

### 3. Run the Application

```bash
python app_x3.py
```

Open http://localhost:5001

## Usage

1. **Sync from X3** - Reads invoices and credit notes from Sage X3
2. **Post to FIRS** - Submits documents to FIRS e-invoicing portal
3. **Download PDF** - Get invoice PDF with IRN and scannable QR code

## Project Structure

```
├── app_x3.py              # Main Flask application
├── sage_x3_reader.py      # Sage X3 SOAP/REST API client
├── config_x3.py           # Configuration (not committed)
├── config_x3.example.py   # Configuration template
├── templates/
│   └── index_x3.html      # Dashboard UI
├── mappings/              # CSV mapping files
│   ├── customer_tin_map.csv
│   ├── hsn_code_map.csv
│   └── product_category_map.csv
└── requirements.txt
```

## API Environments

### Test
- URL: `https://preprod-api.cryptwaresystemsltd.com`

### Production
- URL: `https://api.cryptwaresystemsltd.com`

## Credit Notes

Credit notes require a reference to the original invoice. The system:
1. Looks up the original invoice IRN from the database
2. Includes `cancel_references` array in the API payload
3. Stores the reference for future submissions

## License

Proprietary - Cryptware Systems Ltd
