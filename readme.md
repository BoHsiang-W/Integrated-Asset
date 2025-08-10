# Crypto Assets Management

## Pre-work

1. Log in to your exchange account and create an API key.

    * **OKX:** [Create API Key](https://www.okx.com/account/my-api) | [API Docs](https://www.okx.com/docs-v5/en/)
    * **Bitget:** [Create API Key](https://www.bitget.com/account/newapi) | [API Docs](https://bitgetlimited.github.io/apidoc/en/spot/)
    * **Binance:** [Create API Key](https://www.binance.com/en/my/settings/api-management) | [API Docs](https://developers.binance.com/docs/)

2. Create a `.env` file in your project root and add your API credentials.

    **Important:** Keep your API keys, secrets, and passphrases private. Replace the example values below with your actual credentials:

    ```env
    OKX_API_KEY=your_okx_api_key
    OKX_SECRET_KEY=your_okx_secret_key
    OKX_PASSPHRASE=your_okx_passphrase

    BITGET_API_KEY=your_bitget_api_key
    BITGET_SECRET_KEY=your_bitget_secret_key
    BITGET_PASSPHRASE=your_bitget_passphrase

    BINANCE_API_KEY=your_binance_api_key
    BINANCE_SECRET_KEY=your_binance_secret_key
    ```

    **Note:** Refer to each exchange's API documentation for authentication details.

---

## Gmail API: Save Attachments

1. Open the Google Cloud Console and set API permissions.
2. Enable the Gmail API.

---

## Connect to Gemini with Personal API Key

1. Get your API key from [Google AI Studio](https://aistudio.google.com/apikey).
2. Follow the [Gemini API Quickstart](https://ai.google.dev/gemini-api/docs/quickstart) to verify your key.