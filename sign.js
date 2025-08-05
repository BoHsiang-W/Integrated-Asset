const fetch = require('node-fetch');
const crypto = require('crypto-js');

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
  const response = await fetch(url, { method, headers });
  const data = await response.json();

  if (data.code === '0') {
    // console.log('Account balance:', data.data);
    console.log('USDT Balance:', data.data[0].amt);
    return data.data[0].amt;
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
  const response = await fetch(url, { method, headers });
  const data = await response.json();
  if (data.code === '00000') {
    //   console.log('Account balance:', data.data);
      console.log('USDT Balance:', data.data[0].amount);
      return data.data[0].amount;
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

    const response = await fetch(url, {
        method: 'GET',
        headers: {
            'X-MBX-APIKEY': apiKey,
        },
    });

    const data = await response.json();
    // Find the row for the requested asset (default to 'USDT' if not provided)
    const assetName = asset || 'USDT';
    const assetRow = data.rows.find(row => row.asset === assetName);

    if (assetRow) {
        console.log(`USDT Balance:`, assetRow.totalAmount);
    } else {
        console.log(`Asset ${assetName} not found in response.`);
    }
    return assetRow;
}

getOKXAccountBalance(OKXapiKey, OKXsecretKey, OKXpassphrase);
getBitgetAccountBalance(bitgetapiKey, bitgetsecretKey, bitgetpassphrase);
getBinanceAccountBalance(binanceapiKey, binanceSecretKey);
