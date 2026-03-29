const crypto = require('crypto-js');
const fs = require('fs');
const path = require('path');
const fetch = globalThis.fetch;

const REQUEST_TIMEOUT_MS = 10000;

async function fetchJson(url, options, exchangeName) {
  try {
    const response = await fetch(url, {
      ...options,
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });

    if (!response.ok) {
      const errorBody = await response.text().catch(() => '');
      console.error(`${exchangeName} request failed: HTTP ${response.status} ${response.statusText}`, errorBody);
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

// ---------------------------------------------------------------------------
// Bitget shared auth
// ---------------------------------------------------------------------------

async function bitgetRequest(apiKey, secretKey, passphrase, method, requestPath, label) {
  const timestamp = Date.now().toString();
  const body = '';
  const prehash = timestamp + method + requestPath + body;
  const hash = crypto.HmacSHA256(prehash, secretKey);
  const signature = crypto.enc.Base64.stringify(hash);

  const headers = {
    'ACCESS-KEY': apiKey,
    'ACCESS-SIGN': signature,
    'ACCESS-PASSPHRASE': passphrase,
    'ACCESS-TIMESTAMP': timestamp,
    'locale': 'en-US',
    'Content-Type': 'application/json'
  };

  const url = 'https://api.bitget.com' + requestPath;
  const data = await fetchJson(url, { method, headers }, label);

  if (!data) return null;
  if (data.code === '00000') return data.data;
  console.error(`${label} Error:`, data.msg);
  return null;
}

// ---------------------------------------------------------------------------
// Exchange API functions
// ---------------------------------------------------------------------------

async function getOKXAccountBalance(apiKey, secretKey, passphrase) {
  const timestamp = new Date().toISOString();
  const method = 'GET';
  const requestPath = '/api/v5/finance/savings/balance?ccy=USDT';

  const prehash = timestamp + method + requestPath;
  const hash = crypto.HmacSHA256(prehash, secretKey);
  const signature = crypto.enc.Base64.stringify(hash);

  const headers = {
    'OK-ACCESS-KEY': apiKey,
    'OK-ACCESS-SIGN': signature,
    'OK-ACCESS-TIMESTAMP': timestamp,
    'OK-ACCESS-PASSPHRASE': passphrase,
    'Content-Type': 'application/json'
  };

  const url = 'https://www.okx.com' + requestPath;
  const data = await fetchJson(url, { method, headers }, 'OKX');

  if (!data) return null;

  if (data.code === '0') {
    const balance = data.data?.[0]?.amt;
    if (balance == null) {
      console.error('OKX returned no USDT savings balance.');
      return null;
    }
    console.log('OKX USDT Balance:', balance);
    return balance;
  } else {
    console.error('OKX Error:', data.msg);
    return null;
  }
}

async function getBitgetAccountBalance(apiKey, secretKey, passphrase) {
  const result = await bitgetRequest(apiKey, secretKey, passphrase, 'GET', '/api/v2/earn/account/assets?coin=USDT', 'Bitget');
  const balance = result?.[0]?.amount;
  if (balance == null) {
    console.error('Bitget returned no USDT earn balance.');
    return null;
  }
  console.log('Bitget USDT Balance:', balance);
  return balance;
}

async function getBitgetTradeHistory(apiKey, secretKey, passphrase, symbol, { after, before, limit = '100' } = {}) {
  const queryParams = new URLSearchParams({ symbol, limit });
  if (after) queryParams.set('after', after);
  if (before) queryParams.set('before', before);
  const requestPath = '/api/v2/spot/trade/history-orders?' + queryParams.toString();

  const result = await bitgetRequest(apiKey, secretKey, passphrase, 'GET', requestPath, 'Bitget Trade History');
  if (!result) return null;

  const trades = result.map(t => {
    const baseCoin = t.baseCoin != null ? t.baseCoin : '';
    if (t.baseCoin == null) {
      console.warn('Bitget trade missing baseCoin field for symbol:', t.symbol);
    }
    return {
      symbol: t.symbol,
      priceAvg: t.priceAvg,
      quoteVolume: t.quoteVolume,
      baseCoin,
      uTime: new Date(Number(t.uTime)).toLocaleDateString('sv-SE').replace(/-/g, '/'),
    };
  });
  console.log(`Bitget Trade History (${symbol}): ${trades.length} trade(s)`);
  return trades;
}

async function getBinanceAccountBalance(apiKey, secretKey, asset, current = 1, size = 10) {
  const baseUrl = 'https://api.binance.com';
  const apiPath = '/sapi/v1/simple-earn/flexible/position';
  const timestamp = Date.now();

  const params = new URLSearchParams({
    timestamp: timestamp.toString(),
    asset: asset || '',
    current: current.toString(),
    size: size.toString(),
  });

  const signature = crypto.HmacSHA256(params.toString(), secretKey).toString(crypto.enc.Hex);
  params.append('signature', signature);

  const url = `${baseUrl}${apiPath}?${params.toString()}`;
  const data = await fetchJson(url, {
    method: 'GET',
    headers: { 'X-MBX-APIKEY': apiKey },
  }, 'Binance');

  if (!data) return null;

  const assetName = asset || 'USDT';
  const assetRow = data.rows?.find(row => row.asset === assetName);
  if (assetRow) {
    console.log(`Binance USDT Balance:`, assetRow.totalAmount);
    return assetRow.totalAmount;
  }
  console.log(`Asset ${assetName} not found in response.`);
  return null;
}

// ---------------------------------------------------------------------------
// CSV helpers
// ---------------------------------------------------------------------------

const CSV_PATH = path.join(__dirname, 'attachments', 'stock', 'transactions.csv');
const CSV_HEADER = '交易日期,買/賣/股利,代號,股票,交易類別,買入股數,買入價格,賣出股數,賣出價格,手續費,收入';

/**
 * Parse a CSV row into a key (date, action, code) for dedup.
 * Handles both padded (Python pipeline) and non-padded (sign.js) formats.
 */
function rowKey(line) {
  const cols = line.split(',').map(c => c.trim());
  // Normalize date: strip leading zeros (2026/01/05 → 2026/1/5)
  const parts = cols[0].split('/');
  const date = parts.length === 3
    ? `${parseInt(parts[0])}/${parseInt(parts[1])}/${parseInt(parts[2])}`
    : cols[0];
  return `${date}|${cols[1]}|${cols[2]}`;
}

function readCsvRows() {
  if (!fs.existsSync(CSV_PATH)) return [];
  const lines = fs.readFileSync(CSV_PATH, 'utf-8').replace(/^\uFEFF/, '').trim().split('\n');
  return lines.slice(1).filter(l => l.trim());
}

function writeCsv(rows) {
  rows.sort();
  const seen = new Set();
  const deduped = rows.filter(row => {
    const key = rowKey(row);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  fs.writeFileSync(CSV_PATH, '\uFEFF' + CSV_HEADER + '\n' + deduped.join('\n') + '\n', 'utf-8');
  console.log(`Saved ${CSV_PATH} (${deduped.length} rows)`);
}

function tradesToCsvRows(trades) {
  return trades.map(t => {
    const qty = (parseFloat(t.quoteVolume) / parseFloat(t.priceAvg)).toFixed(6);
    const price = parseFloat(t.priceAvg).toFixed(6);
    return `${t.uTime},買,${t.baseCoin},${t.symbol},Crypto,${qty},${price},,,,`;
  });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

require('dotenv').config();

const BITGET_SYMBOLS = ['BTCUSDT', 'ETHUSDT'];

(async () => {
  const { BITGET_API_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE,
          BINANCE_API_KEY, BINANCE_SECRET_KEY,
          OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE } = process.env;

  const [bitget, binance, okx, ...tradeResults] = await Promise.all([
    getBitgetAccountBalance(BITGET_API_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE),
    getBinanceAccountBalance(BINANCE_API_KEY, BINANCE_SECRET_KEY),
    getOKXAccountBalance(OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE),
    ...BITGET_SYMBOLS.map(s => getBitgetTradeHistory(BITGET_API_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE, s, { limit: '100' })),
  ]);
  console.log(`${bitget}\n${binance}\n${okx}`);

  const allTrades = tradeResults.filter(Boolean).flat();
  if (allTrades.length) {
    const existing = readCsvRows();
    const existingKeys = new Set(existing.map(rowKey));
    const newRows = tradesToCsvRows(allTrades).filter(r => !existingKeys.has(rowKey(r)));
    console.log(`Trades fetched: ${allTrades.length}, new: ${newRows.length}, skipped: ${allTrades.length - newRows.length}`);
    if (newRows.length) {
      writeCsv([...existing, ...newRows]);
    } else {
      console.log('No new trades to add.');
    }
  }
})();
