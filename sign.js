const crypto = require('crypto-js');
const fetch = globalThis.fetch;

const REQUEST_TIMEOUT_MS = 10000;

async function fetchJson(url, options, exchangeName) {
  try {
    const response = await fetch(url, {
      ...options,
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });

    if (!response.ok) {
      console.error(`${exchangeName} request failed: HTTP ${response.status} ${response.statusText}`);
      return null;
    }

    try {
      return await response.json();
    } catch (error) {
      console.error(`${exchangeName} returned an invalid JSON response:`, error.message);
      return null;
    }
  } catch (error) {
    if (error.name === 'TimeoutError') {
      console.error(`${exchangeName} request timed out after ${REQUEST_TIMEOUT_MS}ms`);
      return null;
    }

    console.error(`${exchangeName} request failed:`, error.message);
    return null;
  }
}

async function getOKXAccountBalance(apiKey, secretKey, passphrase) {
  const timestamp = new Date().toISOString();
  const method = 'GET';
  const requestPath = '/api/v5/finance/savings/balance?ccy=USDT';
  const body = '';

  // Create the prehash string
  const prehash = timestamp + method + requestPath + body;

  // Generate the signature
  const hash = crypto.HmacSHA256(prehash, secretKey);
  const signature = crypto.enc.Base64.stringify(hash);

  // Build headers
  const headers = {
    'OK-ACCESS-KEY': apiKey,
    'OK-ACCESS-SIGN': signature,
    'OK-ACCESS-TIMESTAMP': timestamp,
    'OK-ACCESS-PASSPHRASE': passphrase,
    'Content-Type': 'application/json'
  };

  // Make request
  const url = 'https://www.okx.com' + requestPath;
  const data = await fetchJson(url, { method, headers }, 'OKX');

  if (!data) {
    return null;
  }

  // console.log("OKX Response:", data);

  if (data.code === '0') {
    const balance = data.data?.[0]?.amt;

    if (balance == null) {
      console.error('OKX returned no USDT savings balance.');
      return null;
    }

    console.log('OKX USDT Balance:', balance);
    return balance;
  } else {
    console.error('Error:', data.msg);
    return null;
  }
}


async function getBitgetAccountBalance(apiKey, secretKey, passphrase) {
  const timestamp = new Date().toISOString();
  const method = 'GET';
  const requestPath = '/api/v2/earn/account/assets?coin=USDT';
  const body = '';

  // Create the prehash string
  const prehash = timestamp + method + requestPath + body;

  // Generate the signature
  const hash = crypto.HmacSHA256(prehash, secretKey);
  const signature = crypto.enc.Base64.stringify(hash);

  // Build headers
  const headers = {
    'ACCESS-KEY': apiKey,
    'ACCESS-SIGN': signature,
    'ACCESS-PASSPHRASE': passphrase,
    'ACCESS-TIMESTAMP': timestamp,
    'locale': 'en-US',
    'Content-Type': 'application/json'
  };

  // Make request
  const url = 'https://api.bitget.com' + requestPath;
  const data = await fetchJson(url, { method, headers }, 'Bitget');

  if (!data) {
    return null;
  }

  if (data.code === '00000') {
    const balance = data.data?.[0]?.amount;

    if (balance == null) {
      console.error('Bitget returned no USDT earn balance.');
      return null;
    }

      console.log('Bitget USDT Balance:', balance);
      return balance;
  } else {
      console.error('Error:', data.msg);
      return null;
  }
}



async function getBinanceAccountBalance(apiKey, secretKey, asset, current = 1, size = 10) {
    const baseUrl = 'https://api.binance.com';
    const path = '/sapi/v1/simple-earn/flexible/position';
    const timestamp = Date.now();

    // Build query string
    const params = new URLSearchParams({
        timestamp: timestamp.toString(),
        asset: asset || '',
        current: current.toString(),
        size: size.toString(),
    });

    // Create signature using HMAC SHA256 (using crypto-js)
    const signature = crypto.HmacSHA256(params.toString(), secretKey).toString(crypto.enc.Hex);

    params.append('signature', signature);

    const url = `${baseUrl}${path}?${params.toString()}`;

  const data = await fetchJson(url, {
        method: 'GET',
        headers: {
            'X-MBX-APIKEY': apiKey,
        },
  }, 'Binance');

  if (!data) {
    return null;
  }

    // console.log("Binance Response:", data);
    // Find the row for the requested asset (default to 'USDT' if not provided)
    const assetName = asset || 'USDT';
  const assetRow = data.rows?.find(row => row.asset === assetName);

    if (assetRow) {
        console.log(`Binance USDT Balance:`, assetRow.totalAmount);
    } else {
        console.log(`Asset ${assetName} not found in response.`);
    return null;
    }
    return assetRow.totalAmount;
}

require('dotenv').config();

(async () => {
  const [bitget, binance, okx] = await Promise.all([
    getBitgetAccountBalance(process.env.BITGET_API_KEY, process.env.BITGET_SECRET_KEY, process.env.BITGET_PASSPHRASE),
    getBinanceAccountBalance(process.env.BINANCE_API_KEY, process.env.BINANCE_SECRET_KEY),
    getOKXAccountBalance(process.env.OKX_API_KEY, process.env.OKX_SECRET_KEY, process.env.OKX_PASSPHRASE),
  ]);
  console.log(`${bitget}\n${binance}\n${okx}`);
})();

