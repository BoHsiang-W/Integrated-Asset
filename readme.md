# Integrated Asset Management

Automated pipelines for managing stock/ETF transactions, credit-card statements, and crypto exchange trades — with Google Sheets sync.

## Features

| Pipeline | Description |
|----------|-------------|
| **Stock** | Fetch broker PDF statements from Gmail → decrypt → analyze with Gemini → sync to Google Sheets |
| **Credit Card** | Fetch credit-card PDF statements from Gmail → decrypt → analyze with Gemini → CSV output |
| **Crypto** | Fetch balances & trade history from OKX / Bitget / Binance → append to `transactions.csv` |

---

## Pre-work

### 1. Exchange API Keys (for crypto pipeline)

Log in to your exchange account and create an API key:

* **OKX:** [Create API Key](https://www.okx.com/account/my-api) | [API Docs](https://www.okx.com/docs-v5/en/)
* **Bitget:** [Create API Key](https://www.bitget.com/account/newapi) | [API Docs](https://bitgetlimited.github.io/apidoc/en/spot/)
* **Binance:** [Create API Key](https://www.binance.com/en/my/settings/api-management) | [API Docs](https://developers.binance.com/docs/)

### 2. Google Cloud Console

1. Create a project in [Google Cloud Console](https://console.cloud.google.com/).
2. Enable the **Gmail API** and **Google Sheets API**.
3. Create OAuth 2.0 credentials and download `credentials.json` to the project root.

### 3. Gemini API Key

1. Get your API key from [Google AI Studio](https://aistudio.google.com/apikey).
2. Follow the [Gemini API Quickstart](https://ai.google.dev/gemini-api/docs/quickstart) to verify your key.

### 4. Environment Variables

Create a `.env` file in the project root:

> **Important:** Keep all keys, secrets, and passwords private.

```env
# --- Exchange API Keys ---
OKX_API_KEY=your_okx_api_key
OKX_SECRET_KEY=your_okx_secret_key
OKX_PASSPHRASE=your_okx_passphrase

BITGET_API_KEY=your_bitget_api_key
BITGET_SECRET_KEY=your_bitget_secret_key
BITGET_PASSPHRASE=your_bitget_passphrase

BINANCE_API_KEY=your_binance_api_key
BINANCE_SECRET_KEY=your_binance_secret_key

# --- Gemini ---
GOOGLE_API_KEY=your_gemini_api_key

# --- Google Sheets sync ---
GOOGLE_SHEET_ID=your_spreadsheet_id_or_url

# --- PDF passwords (stock brokers) ---
PDF_PASSWORD=your_cathay_pdf_password
FUBON_PDF_PASSWORD=your_fubon_pdf_password

# --- PDF filename patterns (stock brokers) ---
CATHAY_US=regex_for_cathay_us_statements
CATHAY_TW=regex_for_cathay_tw_statements
FUBON_US=regex_for_fubon_us_statements
TW_DIVIDEND=regex_for_tw_dividend_statements

# --- Credit card filename patterns & passwords ---
CATHAY_CARD=regex_for_cathay_card
CATHAY_CARD_PASSWORD=your_password
# ... repeat for FUBON_CARD, TAISHIN_CARD, MEGA_CARD, SINOPAC_CARD,
#     RAKUTEN_CARD, DBS_CARD, LINEBANK_CARD, SCSB_CARD, UBOT_CARD
```

---

## Usage

### Stock Pipeline

```bash
# Run all stages (fetch → decrypt → analyze → sync), default last 7 days
python main.py

# Fetch since a specific date
python main.py --since 2026/01/01

# Run individual stages
python main.py --fetch
python main.py --decrypt
python main.py --analyze
python main.py --sync
```

### Credit Card Pipeline

```bash
# Run all card stages
python main.py --card

# Run individual card stages
python main.py --card --fetch
python main.py --card --decrypt
python main.py --card --analyze
```

### Crypto Pipeline

```bash
node sign.js
```

### Scheduled Execution

```bash
# Windows Task Scheduler (logs to logs/YYYY-MM-DD.log)
run_pipeline.bat
```

---

## Project Structure

```
├── main.py                         # CLI entry point
├── config.py                       # Centralized constants & configs
├── sign.js                         # Crypto exchange balance & trade fetcher
├── run_pipeline.bat                # Task Scheduler entry with logging
│
├── clients/
│   ├── gmail.py                    # Gmail API client with OAuth2
│   ├── gemini.py                   # Gemini AI client with retry
│   └── sheets.py                   # Google Sheets sync writer
│
├── models/
│   └── transaction.py              # Transaction & CardTransaction dataclasses
│
├── pipelines/
│   ├── base.py                     # BasePipeline ABC (fetch → decrypt template)
│   ├── stock.py                    # Stock / ETF / dividend pipeline
│   └── card.py                     # Credit-card pipeline
│
├── brokers/
│   ├── base.py                     # BaseBroker ABC
│   └── ibkr.py                     # IBKR Client Portal API adapter
│
├── utils/
│   ├── csv_helpers.py              # CSV parse, read, write, dedup, normalize
│   ├── pdf_helpers.py              # PDF decrypt & save attachments
│   └── patterns.py                 # Filename pattern matching & tracking
│
├── prompt/                         # Gemini prompt templates
│
└── attachments/
    ├── stock/
    │   ├── raw/                    # Downloaded broker PDFs
    │   ├── decrypted/              # Decrypted broker PDFs
    │   └── transactions.csv        # Merged stock + crypto transactions
    └── card/
        ├── raw/                    # Downloaded card PDFs
        ├── decrypted/              # Decrypted card PDFs
        ├── credit_card_all.csv     # All credit card transactions
        └── monthly_summary.csv     # Monthly spending summary
```