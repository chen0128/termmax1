const process = require('process');
const fs = require('fs');
const dotenv = require('dotenv');
const { promisify } = require('util');
const { execFile } = require('child_process');
const {
  Wallet,
  JsonRpcProvider,
  Contract,
  formatUnits,
  parseUnits,
  FetchRequest,
} = require('ethers');
const { HttpsProxyAgent } = require('https-proxy-agent');

dotenv.config({ quiet: true });

const API_BASE = 'https://leaderboard-api-v2.termmax.ts.finance';
const DEFAULT_CONCURRENCY = 5;
const DEFAULT_CHECKIN_CONTRACT = '0x007200c66bd2a5bd7c744b90df8ecbeb34fd26d4';
const DEFAULT_CHECKIN_DATA = '0x183ff085';
const DEFAULT_USDT_CONTRACT = '0x55d398326f99059ff775485246999027b3197955';
const DEFAULT_USDC_CONTRACT = '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d';
const DEFAULT_TZ = 'Asia/Shanghai';
const DEFAULT_CHECKIN_HISTORY_TZ = 'UTC';
const EXPLORER_GAS_TRACKER_URLS = {
  1: 'https://etherscan.io/gastracker',
  56: 'https://bscscan.com/gastracker',
  137: 'https://polygonscan.com/gastracker',
  8453: 'https://basescan.org/gastracker',
  42161: 'https://arbiscan.io/gastracker',
};
const ERC20_ABI = [
  'function balanceOf(address account) view returns (uint256)',
  'function decimals() view returns (uint8)',
];

let cachedProvider = null;
const execFileAsync = promisify(execFile);

function getEnv(name, fallback = '') {
  const value = process.env[name];
  if (typeof value === 'undefined') return fallback;
  const trimmed = String(value).trim();
  return trimmed || fallback;
}


function parseBoolean(value, fallback = false) {
  if (typeof value === 'undefined' || value === null || String(value).trim() === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) return false;
  return fallback;
}

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseFloatOrNull(value) {
  const parsed = Number.parseFloat(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizePrivateKey(value, label = '') {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error(`Private key is empty${label ? ` (${label})` : ''}`);
  }
  const normalized = raw.startsWith('0x') ? raw : `0x${raw}`;
  if (!/^0x[0-9a-fA-F]{64}$/.test(normalized)) {
    throw new Error(`Invalid private key format${label ? ` (${label})` : ''}`);
  }
  return normalized;
}

function parseWindow(windowStr) {
  if (!windowStr) return null;
  const parts = windowStr.split('-');
  if (parts.length !== 2) {
    throw new Error('RANDOM_WINDOW format invalid, expected HH:MM-HH:MM');
  }
  return { start: parts[0].trim(), end: parts[1].trim() };
}

function parseTimeToMinutes(t) {
  const [hh, mm] = t.split(':').map((v) => Number.parseInt(v, 10));
  if (Number.isNaN(hh) || Number.isNaN(mm)) {
    throw new Error(`Invalid time: ${t}`);
  }
  return hh * 60 + mm;
}

function minutesToMs(m) {
  return m * 60 * 1000;
}

function nowMinutes(tz) {
  if (!tz) {
    const d = new Date();
    return d.getHours() * 60 + d.getMinutes();
  }
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
  }).formatToParts(new Date());
  const hh = Number.parseInt(parts.find((p) => p.type === 'hour')?.value || '0', 10);
  const mm = Number.parseInt(parts.find((p) => p.type === 'minute')?.value || '0', 10);
  return hh * 60 + mm;
}

function computeDelayMs(windowStr, tz) {
  const windowConfig = parseWindow(windowStr);
  if (!windowConfig) return 0;

  const startMin = parseTimeToMinutes(windowConfig.start);
  const endMin = parseTimeToMinutes(windowConfig.end);
  if (endMin === startMin) return 0;

  const span = endMin >= startMin
    ? endMin - startMin
    : (1440 - startMin) + endMin;

  const randOffset = Math.floor(Math.random() * span);
  const targetMin = (startMin + randOffset) % 1440;

  const nowMin = nowMinutes(tz);
  const diffMin = targetMin >= nowMin
    ? targetMin - nowMin
    : (1440 - nowMin) + targetMin;

  return minutesToMs(diffMin);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getUserAgent() {
  return getEnv('USER_AGENT')
    || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36';
}

function defaultHeaders() {
  const headers = {
    accept: 'application/json',
    'content-type': 'application/json',
    'user-agent': getUserAgent(),
  };

  if (process.env.ORIGIN) headers.origin = process.env.ORIGIN;
  if (process.env.REFERER) headers.referer = process.env.REFERER;
  if (process.env.SEC_CH_UA) headers['sec-ch-ua'] = process.env.SEC_CH_UA;
  if (process.env.SEC_CH_UA_MOBILE) headers['sec-ch-ua-mobile'] = process.env.SEC_CH_UA_MOBILE;
  if (process.env.SEC_CH_UA_PLATFORM) headers['sec-ch-ua-platform'] = process.env.SEC_CH_UA_PLATFORM;
  if (process.env.ACCEPT_LANGUAGE) headers['accept-language'] = process.env.ACCEPT_LANGUAGE;
  if (process.env.PRIORITY) headers.priority = process.env.PRIORITY;
  if (process.env.SEC_FETCH_DEST) headers['sec-fetch-dest'] = process.env.SEC_FETCH_DEST;
  if (process.env.SEC_FETCH_MODE) headers['sec-fetch-mode'] = process.env.SEC_FETCH_MODE;
  if (process.env.SEC_FETCH_SITE) headers['sec-fetch-site'] = process.env.SEC_FETCH_SITE;

  return headers;
}

function buildProxyAgent(proxy) {
  if (!proxy) return null;
  const raw = String(proxy).trim();
  if (!raw) return null;

  if (/^https?:\/\//i.test(raw)) {
    return new HttpsProxyAgent(raw);
  }

  const parts = raw.split(':');
  if (parts.length === 2) {
    const [host, port] = parts;
    return new HttpsProxyAgent(`http://${host}:${port}`);
  }

  if (parts.length === 4) {
    const [host, port, username, password] = parts;
    const proxyUrl = `http://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${host}:${port}`;
    return new HttpsProxyAgent(proxyUrl);
  }

  throw new Error('PROXY format invalid, expected host:port or host:port:username:password');
}

function getTimeoutMs() {
  return Math.max(1_000, parseInteger(process.env.HTTP_TIMEOUT_MS || '30000', 30000));
}

async function httpRequest(url, options = {}, proxy = '') {
  const reqOptions = {
    ...options,
    headers: {
      ...defaultHeaders(),
      ...(options.headers || {}),
    },
  };

  const agent = buildProxyAgent(proxy);
  if (agent) {
    reqOptions.agent = agent;
  }

  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function') {
    reqOptions.signal = AbortSignal.timeout(getTimeoutMs());
  }

  const response = await fetch(url, reqOptions);
  const text = await response.text();

  return {
    status: response.status,
    headers: response.headers,
    text,
  };
}

async function httpJson(url, options = {}, proxy = '') {
  const response = await httpRequest(url, options, proxy);
  let data = null;
  try {
    data = response.text ? JSON.parse(response.text) : null;
  } catch {
    data = response.text;
  }

  return {
    status: response.status,
    data,
    text: response.text,
  };
}

function readLines(filePath) {
  if (!fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'));
}

function formatDateInTz(date, tz) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz || undefined,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const year = parts.find((p) => p.type === 'year')?.value || '';
  const month = parts.find((p) => p.type === 'month')?.value || '';
  const day = parts.find((p) => p.type === 'day')?.value || '';
  return `${year}-${month}-${day}`;
}

function normalizeDate(value) {
  if (!value && value !== 0) return '';
  const str = String(value).trim();
  if (!str) return '';
  return str.slice(0, 10).replace(/\//g, '-');
}

function getCheckinTargetDates(tz) {
  const today = formatDateInTz(new Date(), tz);
  return [today];
}

function coerceBoolean(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'y', 'checked', 'checked_in', 'success'].includes(normalized)) {
      return true;
    }
    if (['false', '0', 'no', 'n', 'unchecked', 'failed'].includes(normalized)) {
      return false;
    }
  }
  return null;
}

function isHistoryItemCheckedIn(item) {
  if (item == null) return false;
  if (typeof item === 'string' || typeof item === 'number') return true;

  const boolFields = [
    item.isCheckedIn,
    item.checkedIn,
    item.isCheckIn,
    item.success,
    item.status,
  ];
  for (const value of boolFields) {
    const parsed = coerceBoolean(value);
    if (parsed !== null) {
      return parsed;
    }
  }

  return true;
}

function extractCheckinEntries(historyData) {
  const arrayCandidates = [
    historyData,
    historyData?.data,
    historyData?.data?.list,
    historyData?.data?.records,
    historyData?.data?.result,
    historyData?.data?.history,
    historyData?.data?.checkins,
    historyData?.data?.dates,
    historyData?.list,
    historyData?.records,
    historyData?.result,
    historyData?.history,
    historyData?.checkins,
    historyData?.dates,
  ];

  for (const candidate of arrayCandidates) {
    if (!Array.isArray(candidate)) continue;

    const entries = candidate
      .map((item) => {
        if (item == null) return null;

        if (typeof item === 'string' || typeof item === 'number') {
          const date = normalizeDate(item);
          return date ? { date, checkedIn: true } : null;
        }

        const date = normalizeDate(
          item.date
          || item.checkinDate
          || item.day
          || item.createdAt
          || item.timestamp
          || item.time
        );

        if (!date) return null;
        return {
          date,
          checkedIn: isHistoryItemCheckedIn(item),
        };
      })
      .filter(Boolean);

    if (entries.length > 0) {
      return entries;
    }
  }

  const singleDate = normalizeDate(
    historyData?.data?.date
    || historyData?.date
    || historyData?.data?.checkinDate
    || historyData?.checkinDate
  );
  const singleCheckedIn = coerceBoolean(
    historyData?.data?.isCheckedIn
    ?? historyData?.isCheckedIn
    ?? historyData?.data?.checkedIn
    ?? historyData?.checkedIn
  );

  if (!singleDate) return [];
  return [{ date: singleDate, checkedIn: singleCheckedIn ?? true }];
}

async function fetchCheckinHistory(walletAddress, startDate, endDate, proxy) {
  const historyUrl = `${API_BASE}/leaderboard/daily/checkin-history?userAddress=${walletAddress}&startDate=${startDate}&endDate=${endDate}`;
  const response = await httpJson(historyUrl, { method: 'GET' }, proxy);
  if (response.status !== 200) {
    throw new Error(`Failed to get checkin history: ${response.status} ${JSON.stringify(response.data)}`);
  }
  return response.data;
}

async function checkWallet({ walletAddress, proxy, todayStr, targetDates }) {
  const dates = (Array.isArray(targetDates) && targetDates.length > 0)
    ? targetDates.map((item) => normalizeDate(item)).filter(Boolean)
    : [normalizeDate(todayStr)].filter(Boolean);

  if (dates.length === 0) {
    throw new Error('No target date provided for checkin-history query');
  }

  const sortedDates = [...new Set(dates)].sort();
  const startDate = sortedDates[0];
  const endDate = sortedDates[sortedDates.length - 1];

  const historyData = await fetchCheckinHistory(walletAddress, startDate, endDate, proxy);
  const entries = extractCheckinEntries(historyData);

  const checkedInToday = sortedDates.some((targetDate) => entries
    .filter((entry) => entry.date === targetDate)
    .some((entry) => entry.checkedIn));

  return {
    walletAddress,
    checkedInToday,
    targetDates: sortedDates,
    entries,
    historyData,
  };
}

async function fetchSignInMessage(walletAddress, proxy) {
  const url = `${API_BASE}/v2/board/wallet/sign-in-message?walletAddress=${walletAddress}`;
  const response = await httpJson(url, { method: 'GET' }, proxy);
  if (response.status !== 200) {
    throw new Error(`Failed to get sign-in message: ${response.status} ${JSON.stringify(response.data)}`);
  }
  if (!response.data?.message || !response.data?.timestamp) {
    throw new Error(`Unexpected sign-in-message response: ${JSON.stringify(response.data)}`);
  }
  return response.data;
}

async function submitSignIn(payload, proxy) {
  const url = `${API_BASE}/v2/board/wallet/sign-in`;
  const response = await httpJson(
    url,
    {
      method: 'POST',
      body: JSON.stringify(payload),
    },
    proxy
  );

  if (response.status !== 200 && response.status !== 201) {
    throw new Error(`Failed to submit sign-in: ${response.status} ${JSON.stringify(response.data)}`);
  }

  return response.data;
}

function walletAddressFromPrivateKey(privateKey, indexLabel) {
  try {
    return new Wallet(privateKey).address;
  } catch {
    const suffix = indexLabel ? ` (${indexLabel})` : '';
    throw new Error(`Invalid private key${suffix}`);
  }
}

function splitRpcCandidates(raw) {
  if (!raw) return [];
  return String(raw)
    .split(/[\r\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

async function getRpcProvider() {
  if (cachedProvider) return cachedProvider;

  const candidates = [];
  const seen = new Set();

  const appendCandidates = (items) => {
    for (const rpc of items) {
      if (seen.has(rpc)) continue;
      seen.add(rpc);
      candidates.push(rpc);
    }
  };

  appendCandidates(splitRpcCandidates(getEnv('RPC_URL')));
  appendCandidates(splitRpcCandidates(getEnv('RPC_URLS')));
  appendCandidates(splitRpcCandidates(getEnv('BSC_RPC')));

  if (candidates.length === 0) {
    throw new Error('Missing RPC URL. Set RPC_URL / RPC_URLS / BSC_RPC in .env');
  }

  const timeoutMs = getTimeoutMs();
  let lastError = null;

  for (const rpcUrl of candidates) {
    try {
      const request = new FetchRequest(rpcUrl);
      request.timeout = timeoutMs;
      const provider = new JsonRpcProvider(request);
      await provider.getNetwork();
      cachedProvider = provider;
      return cachedProvider;
    } catch (error) {
      lastError = error;
      console.warn(`[WARN] RPC unavailable: ${rpcUrl} -> ${error?.message || error}`);
    }
  }

  throw new Error(`All RPC endpoints failed (${candidates.length}). Last error: ${lastError?.message || lastError}`);
}

async function readTokenBalance(provider, tokenAddress, walletAddress) {
  const contract = new Contract(tokenAddress, ERC20_ABI, provider);
  const [rawBalance, decimals] = await Promise.all([
    contract.balanceOf(walletAddress),
    contract.decimals(),
  ]);

  return {
    rawBalance,
    decimals,
    value: Number(formatUnits(rawBalance, decimals)),
  };
}

async function maybeLogBalances(provider, walletAddress) {
  const minBnb = parseFloatOrNull(process.env.MIN_BNB);
  const minUsdt = parseFloatOrNull(process.env.MIN_USDT);
  const minUsdc = parseFloatOrNull(process.env.MIN_USDC);

  const usdtContract = getEnv('USDT_CONTRACT', DEFAULT_USDT_CONTRACT);
  const usdcContract = getEnv('USDC_CONTRACT', DEFAULT_USDC_CONTRACT);

  try {
    const nativeBalance = Number(formatUnits(await provider.getBalance(walletAddress), 18));
    const [usdt, usdc] = await Promise.all([
      readTokenBalance(provider, usdtContract, walletAddress),
      readTokenBalance(provider, usdcContract, walletAddress),
    ]);

    const bnbOk = minBnb === null ? null : nativeBalance >= minBnb;
    const usdtOk = minUsdt === null ? null : usdt.value >= minUsdt;
    const usdcOk = minUsdc === null ? null : usdc.value >= minUsdc;

    const qualified = [bnbOk, usdtOk, usdcOk].some((flag) => flag === true);

    console.log(
      `[BALANCE] BNB=${nativeBalance.toFixed(6)} USDT=${usdt.value.toFixed(4)} USDC=${usdc.value.toFixed(4)}`
    );

    if ([bnbOk, usdtOk, usdcOk].every((flag) => flag !== null)) {
      console.log(`[BALANCE] Reward threshold met: ${qualified ? 'YES' : 'NO'}`);
    }
  } catch (error) {
    console.warn(`[WARN] Failed to read balances for ${walletAddress}: ${error.message || error}`);
  }
}

async function doApiSignIn({ signer, walletAddress, proxy }) {
  const signInMessage = await fetchSignInMessage(walletAddress, proxy);
  const signature = await signer.signMessage(signInMessage.message);

  const payload = {
    walletAddress,
    signature,
    id: getEnv('CLIENT_ID', 'termmax-script'),
    name: getEnv('CLIENT_NAME', 'TermMax Script'),
    userAgent: getUserAgent(),
    timestamp: signInMessage.timestamp,
  };


  return submitSignIn(payload, proxy);
}

function getExplorerGasTrackerUrl(chainId) {
  const customUrl = getEnv('EXPLORER_GAS_URL');
  if (customUrl) return customUrl;
  return EXPLORER_GAS_TRACKER_URLS[chainId] || '';
}

function extractGasGweiFromHtml(html) {
  const patterns = [
    /<title>\s*([0-9]+(?:\.[0-9]+)?)\s*Gwei/i,
    /Gas Snapshot Price[^-]*-\s*([0-9]+(?:\.[0-9]+)?)\s*Gwei/i,
    /"gasPrice"\s*:\s*"?([0-9]+(?:\.[0-9]+)?)/i,
  ];

  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (!match) continue;
    const value = Number.parseFloat(match[1]);
    if (Number.isFinite(value) && value > 0) {
      return value;
    }
  }

  return null;
}

async function fetchExplorerHtmlViaPowerShell(url) {
  if (process.platform !== 'win32') return '';

  const psCommand = `(Invoke-WebRequest -Uri '${url}' -UseBasicParsing -TimeoutSec 20).Content`;
  const { stdout } = await execFileAsync(
    'powershell.exe',
    ['-NoProfile', '-Command', psCommand],
    { maxBuffer: 8 * 1024 * 1024 }
  );

  return String(stdout || '');
}

async function fetchExplorerGasPriceGwei(chainId, proxy) {
  const url = getExplorerGasTrackerUrl(chainId);
  if (!url) {
    throw new Error(`No explorer gas tracker URL configured for chainId=${chainId}`);
  }

  let html = '';
  try {
    const response = await httpRequest(url, { method: 'GET' }, proxy);
    if (response.status === 200) {
      html = response.text || '';
    }
  } catch {
    html = '';
  }

  let gasGwei = extractGasGweiFromHtml(html);
  if (gasGwei !== null) {
    return gasGwei;
  }

  // Cloudflare challenge pages are common for fetch() on explorer domains.
  const psHtml = await fetchExplorerHtmlViaPowerShell(url);
  gasGwei = extractGasGweiFromHtml(psHtml);
  if (gasGwei !== null) {
    return gasGwei;
  }

  throw new Error('Unable to parse gas price from explorer response');
}

async function resolveGasPrice(provider, proxy) {
  const customGas = parseFloatOrNull(process.env.GAS_PRICE_GWEI);
  if (customGas !== null && customGas > 0) {
    return {
      source: 'custom',
      gwei: customGas,
      wei: parseUnits(String(customGas), 'gwei'),
    };
  }

  const gasSource = getEnv('GAS_SOURCE', 'explorer').toLowerCase();
  const network = await provider.getNetwork();
  const chainId = Number(network.chainId);

  if (gasSource === 'explorer') {
    try {
      const gasGwei = await fetchExplorerGasPriceGwei(chainId, proxy);
      return {
        source: 'explorer',
        gwei: gasGwei,
        wei: parseUnits(String(gasGwei), 'gwei'),
      };
    } catch (error) {
      console.warn(`[WARN] Explorer gas fetch failed, fallback to RPC fee data: ${error.message || error}`);
    }
  }

  const feeData = await provider.getFeeData();
  const gasPriceWei = feeData.gasPrice || feeData.maxFeePerGas;
  if (!gasPriceWei) {
    throw new Error('Unable to resolve gas price from RPC fee data');
  }

  return {
    source: 'rpc',
    gwei: Number(formatUnits(gasPriceWei, 'gwei')),
    wei: gasPriceWei,
  };
}

async function doOnchainCheckIn(signer, proxy) {
  const txRequest = {
    to: getEnv('CHECKIN_CONTRACT', DEFAULT_CHECKIN_CONTRACT),
    data: getEnv('CHECKIN_DATA', DEFAULT_CHECKIN_DATA),
  };

  if (!txRequest.to) {
    throw new Error('Missing CHECKIN_CONTRACT');
  }

  if (!txRequest.data || !txRequest.data.startsWith('0x')) {
    throw new Error('CHECKIN_DATA must be hex data, e.g. 0x183ff085');
  }

  if (signer.provider) {
    const gas = await resolveGasPrice(signer.provider, proxy);
    txRequest.gasPrice = gas.wei;
    console.log(`[GAS] source=${gas.source} gas=${gas.gwei.toFixed(3)} gwei`);
  }

  const gasLimit = parseInteger(process.env.GAS_LIMIT || '', null);
  if (gasLimit !== null && gasLimit > 0) {
    txRequest.gasLimit = BigInt(gasLimit);
  }

  const txResponse = await signer.sendTransaction(txRequest);
  const waitConfirmations = Math.max(0, parseInteger(process.env.WAIT_CONFIRMATIONS || '1', 1));
  const receipt = await txResponse.wait(waitConfirmations);

  if (!receipt || Number(receipt.status) !== 1) {
    throw new Error(`On-chain check-in transaction reverted: ${txResponse.hash}`);
  }

  return {
    txHash: txResponse.hash,
    receipt,
  };
}

async function pollCheckinStatus({ walletAddress, proxy, checkinHistoryTz, retries, intervalMs }) {
  let latest = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const targetDates = getCheckinTargetDates(checkinHistoryTz);
    latest = await checkWallet({ walletAddress, proxy, targetDates });
    if (latest.checkedInToday) {
      return latest;
    }

    if (attempt < retries) {
      await sleep(intervalMs);
    }
  }

  return latest;
}

function loadAccounts() {
  const privateKeysRaw = readLines('private_keys.txt');
  const proxies = readLines('proxies.txt');

  if (privateKeysRaw.length > 0) {
    const privateKeys = privateKeysRaw.map((value, index) => normalizePrivateKey(value, `line ${index + 1}`));

    if (proxies.length > 0 && proxies.length !== privateKeys.length) {
      throw new Error('private_keys.txt and proxies.txt must have the same number of lines');
    }


    return privateKeys.map((privateKey, index) => {
      const walletAddress = walletAddressFromPrivateKey(privateKey, `line ${index + 1}`);

      return {
        privateKey,
        walletAddress,
        proxy: proxies[index] || '',
      };
    });
  }

  const privateKeyFromEnv = getEnv('PRIVATE_KEY');
  if (privateKeyFromEnv) {
    const privateKey = normalizePrivateKey(privateKeyFromEnv, 'PRIVATE_KEY');
    const walletAddress = walletAddressFromPrivateKey(privateKey, 'PRIVATE_KEY');
    const walletAddressEnv = getEnv('WALLET_ADDRESS');

    if (walletAddressEnv && walletAddressEnv.toLowerCase() !== walletAddress.toLowerCase()) {
      throw new Error(`WALLET_ADDRESS (${walletAddressEnv}) does not match PRIVATE_KEY derived address (${walletAddress})`);
    }

    return [{
      privateKey,
      walletAddress,
      proxy: getEnv('PROXY'),
    }];
  }

  const hints = [];
  if (proxies.length > 0) hints.push(`proxies.txt has ${proxies.length} proxies`);

  const hintText = hints.length > 0 ? ` (${hints.join('; ')})` : '';
  throw new Error(`No private key source found. Provide private_keys.txt or PRIVATE_KEY in .env${hintText}`);
}

async function runWithConcurrency(items, limit, handler) {
  if (items.length === 0) return [];
  const results = new Array(items.length);
  const workerCount = Math.min(limit, items.length);
  let nextIndex = 0;

  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= items.length) break;
      results[index] = await handler(items[index], index);
    }
  });

  await Promise.all(workers);
  return results;
}

function shuffleArray(items) {
  const copied = [...items];
  for (let i = copied.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copied[i], copied[j]] = [copied[j], copied[i]];
  }
  return copied;
}

function parseOrderMode(value) {
  const mode = String(value || '').trim().toLowerCase();
  if (!mode || mode === 'sequential' || mode === '\u987a\u5e8f') return 'sequential';
  if (mode === 'random' || mode === '\u968f\u673a') return 'random';
  throw new Error(`Invalid CHECKIN_ORDER: ${value}. Expected sequential/random or zh labels`);
}

function parseIntervalMode(value) {
  const mode = String(value || '').trim().toLowerCase();
  if (!mode || mode === 'none' || mode === '\u65e0\u95f4\u9694') return 'none';
  if (mode === 'fixed' || mode === '\u56fa\u5b9a\u95f4\u9694') return 'fixed';
  if (mode === 'random' || mode === '\u968f\u673a\u95f4\u9694') return 'random';
  throw new Error(`Invalid INTERVAL_MODE: ${value}. Expected none/fixed/random or zh labels`);
}

function getIntervalSeconds(config) {
  if (config.mode === 'none') return 0;
  if (config.mode === 'fixed') return config.fixedSec;
  const range = config.maxSec - config.minSec;
  return config.minSec + Math.random() * range;
}

async function processAccount(account, index, total, context) {
  const { provider, checkinHistoryTz } = context;
  console.log(`
[${index + 1}/${total}] ${account.walletAddress}`);

  const before = await checkWallet({
    walletAddress: account.walletAddress,
    proxy: account.proxy,
    targetDates: getCheckinTargetDates(checkinHistoryTz),
  });

  if (before.checkedInToday) {
    console.log(`[SKIP] ${account.walletAddress} already checked in today.`);
    return {
      walletAddress: account.walletAddress,
      checkedInToday: true,
      state: 'already_checked',
      txHash: '',
    };
  }

  if (context.dryRun) {
    console.log(`[DRY_RUN] ${account.walletAddress} not checked in. Would execute sign-in/check-in now.`);
    return {
      walletAddress: account.walletAddress,
      checkedInToday: false,
      state: 'dry_run',
      txHash: '',
    };
  }

  const signer = provider
    ? new Wallet(account.privateKey, provider)
    : new Wallet(account.privateKey);

  if (context.checkBalance) {
    if (!provider) {
      throw new Error('RPC provider is not initialized for balance checks');
    }
    await maybeLogBalances(provider, account.walletAddress);
  }

  if (context.doApiSignIn) {
    await doApiSignIn({
      signer,
      walletAddress: account.walletAddress,
      proxy: account.proxy,
    });
    console.log(`[API] ${account.walletAddress} sign-in submitted.`);
  }

  if (context.doOnchainCheckin) {
    const preOnchain = await checkWallet({
      walletAddress: account.walletAddress,
      proxy: account.proxy,
      targetDates: getCheckinTargetDates(checkinHistoryTz),
    });

    if (preOnchain.checkedInToday) {
      console.log(`[SKIP] ${account.walletAddress} already checked in, skip on-chain step.`);
      return {
        walletAddress: account.walletAddress,
        checkedInToday: true,
        state: context.doApiSignIn ? 'api_signed' : 'already_checked',
        txHash: '',
      };
    }
  }

  let txHash = '';
  if (context.doOnchainCheckin) {
    const onchain = await doOnchainCheckIn(signer, account.proxy);
    txHash = onchain.txHash;
    console.log(`[TX] ${account.walletAddress} ${txHash}`);
  }

  if (context.doOnchainCheckin && context.postOnchainWaitMs > 0) {
    await sleep(context.postOnchainWaitMs);
  }

  if (context.doOnchainCheckin) {
    const after = await pollCheckinStatus({
      walletAddress: account.walletAddress,
      proxy: account.proxy,
      checkinHistoryTz,
      retries: context.pollRetries,
      intervalMs: context.pollIntervalMs,
    });

    if (!after || !after.checkedInToday) {
      const dateHint = after?.targetDates?.join(', ') || 'n/a';
      throw new Error(`Check-in not confirmed by checkin-history API (dates=${dateHint}, tx=${txHash || 'n/a'})`);
    }

    console.log(`[OK] ${account.walletAddress} checked in successfully.`);
    return {
      walletAddress: account.walletAddress,
      checkedInToday: true,
      state: 'checked_in',
      txHash,
    };
  }

  console.log(`[OK] ${account.walletAddress} API sign-in completed.`);
  return {
    walletAddress: account.walletAddress,
    checkedInToday: true,
    state: 'api_signed',
    txHash,
  };
}

function buildFailedResult(account, message) {
  return {
    walletAddress: account.walletAddress,
    checkedInToday: false,
    state: 'failed',
    txHash: '',
    error: message,
  };
}

async function processAccountWithRetry(account, index, total, context, retryTimes) {
  let lastMessage = 'Unknown error';

  for (let attempt = 0; attempt <= retryTimes; attempt += 1) {
    try {
      return await processAccount(account, index, total, context);
    } catch (error) {
      lastMessage = error?.message || String(error);
      const isLastAttempt = attempt >= retryTimes;
      if (isLastAttempt) {
        console.log(`[FAILED] ${account.walletAddress} - ${lastMessage}`);
        return buildFailedResult(account, lastMessage);
      }

      console.log(`[RETRY] ${account.walletAddress} failed: ${lastMessage}. next retry ${attempt + 1}/${retryTimes}`);
    }
  }

  return buildFailedResult(account, lastMessage);
}

async function main() {
  const randomWindow = getEnv('RANDOM_WINDOW');
  const tz = getEnv('TZ', DEFAULT_TZ);
  const checkinHistoryTz = getEnv('CHECKIN_HISTORY_TZ', DEFAULT_CHECKIN_HISTORY_TZ);
  const delayMs = computeDelayMs(randomWindow, tz);

  const doOnchainCheckin = parseBoolean(process.env.DO_ONCHAIN_CHECKIN, true);
  const doApiSignIn = parseBoolean(process.env.DO_API_SIGN_IN, false);
  const dryRun = parseBoolean(process.env.DRY_RUN, false);
  const checkBalance = parseBoolean(process.env.CHECK_BALANCE, false);

  if (!doOnchainCheckin && !doApiSignIn) {
    throw new Error('Nothing to execute: both DO_ONCHAIN_CHECKIN and DO_API_SIGN_IN are false');
  }

  const requestedConcurrency = Math.max(
    1,
    parseInteger(process.env.CONCURRENCY || `${DEFAULT_CONCURRENCY}`, DEFAULT_CONCURRENCY)
  );
  const orderMode = parseOrderMode(getEnv('CHECKIN_ORDER', 'sequential'));
  const intervalMode = parseIntervalMode(getEnv('INTERVAL_MODE', 'none'));

  let effectiveConcurrency = requestedConcurrency;
  if (intervalMode !== 'none' && requestedConcurrency > 1) {
    console.warn('[WARN] INTERVAL_MODE requires sequential execution. CONCURRENCY forced to 1.');
    effectiveConcurrency = 1;
  }

  const intervalConfig = {
    mode: intervalMode,
    fixedSec: Math.max(0, parseFloatOrNull(process.env.INTERVAL_SEC) ?? 0),
    minSec: Math.max(0, parseFloatOrNull(process.env.INTERVAL_MIN_SEC) ?? 0),
    maxSec: Math.max(0, parseFloatOrNull(process.env.INTERVAL_MAX_SEC) ?? 0),
  };
  if (intervalConfig.mode === 'random' && intervalConfig.maxSec < intervalConfig.minSec) {
    [intervalConfig.minSec, intervalConfig.maxSec] = [intervalConfig.maxSec, intervalConfig.minSec];
  }

  const postOnchainWaitMs = Math.max(0, parseInteger(process.env.POST_ONCHAIN_WAIT_MS || '5000', 5000));
  const pollRetries = Math.max(0, parseInteger(process.env.POLL_RETRIES || '2', 2));
  const pollIntervalMs = Math.max(500, parseInteger(process.env.POLL_INTERVAL_MS || '5000', 5000));
  const batchWaitSec = Math.max(0, parseFloatOrNull(process.env.BATCH_WAIT_SEC) ?? 0);
  const retryTimes = Math.max(0, parseInteger(process.env.RETRY_TIMES || '0', 0));

  if (delayMs > 0) {
    console.log(`Waiting ${Math.round(delayMs / 1000)}s before executing...`);
    await sleep(delayMs);
  }

  const displayDates = getCheckinTargetDates(checkinHistoryTz);
  const displayDateText = displayDates.join(',');
  const loadedAccounts = loadAccounts();

  if (loadedAccounts.length === 0) {
    throw new Error('No account available for processing');
  }

  const accounts = orderMode === 'random' ? shuffleArray(loadedAccounts) : [...loadedAccounts];

  const needsProvider = !dryRun && (doOnchainCheckin || checkBalance);

  let provider = null;
  if (needsProvider) {
    provider = await getRpcProvider();
    const network = await provider.getNetwork();
    console.log(`Using RPC chainId=${network.chainId.toString()}, accounts=${accounts.length}, dates=${displayDateText}, runTz=${tz}, checkinTz=${checkinHistoryTz}`);
  } else {
    console.log(`Using accounts=${accounts.length}, dates=${displayDateText}, runTz=${tz}, checkinTz=${checkinHistoryTz}${dryRun ? ' (DRY_RUN)' : ''}`);
  }

  console.log(`Order=${orderMode}, concurrency=${effectiveConcurrency}, intervalMode=${intervalMode}`);
  if (effectiveConcurrency > 1) {
    console.log('Batch mode=strict (wait each batch completed before next batch).');
  }
  if (batchWaitSec > 0) {
    console.log(`Batch wait=${batchWaitSec}s`);
  }
  if (retryTimes > 0) {
    console.log(`Retry times=${retryTimes}`);
  }
  if (intervalMode === 'fixed') {
    console.log(`Interval=${intervalConfig.fixedSec}s`);
  }
  if (intervalMode === 'random') {
    console.log(`Interval=random ${intervalConfig.minSec}s~${intervalConfig.maxSec}s`);
  }

  const context = {
    checkinHistoryTz,
    provider,
    doOnchainCheckin,
    doApiSignIn,
    checkBalance,
    postOnchainWaitMs,
    pollRetries,
    pollIntervalMs,
    dryRun,
  };

  const results = [];

  if (effectiveConcurrency === 1) {
    for (let i = 0; i < accounts.length; i += 1) {
      const account = accounts[i];
      const result = await processAccountWithRetry(account, i, accounts.length, context, retryTimes);
      results.push(result);

      if (i < accounts.length - 1) {
        const intervalSec = getIntervalSeconds(intervalConfig);
        if (intervalSec > 0) {
          console.log(`[WAIT] sleeping ${intervalSec.toFixed(2)}s before next account.`);
          await sleep(Math.round(intervalSec * 1000));
        }
      }
    }
  } else {
    const totalBatches = Math.ceil(accounts.length / effectiveConcurrency);
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex += 1) {
      const start = batchIndex * effectiveConcurrency;
      const end = Math.min(accounts.length, start + effectiveConcurrency);
      const batchAccounts = accounts.slice(start, end);

      const batchResults = await Promise.all(batchAccounts.map(async (account, offset) => {
        const index = start + offset;
        return await processAccountWithRetry(account, index, accounts.length, context, retryTimes);
      }));

      results.push(...batchResults);

      const isLastBatch = batchIndex >= totalBatches - 1;
      if (!isLastBatch && batchWaitSec > 0) {
        console.log(`[BATCH_WAIT] sleeping ${batchWaitSec.toFixed(2)}s before next batch.`);
        await sleep(Math.round(batchWaitSec * 1000));
      }
    }
  }

  const checked = results.filter((item) => item.state === 'checked_in').length;
  const apiSigned = results.filter((item) => item.state === 'api_signed').length;
  const already = results.filter((item) => item.state === 'already_checked').length;
  const dryRuns = results.filter((item) => item.state === 'dry_run').length;
  const failed = results.filter((item) => item.state === 'failed');

  console.log('');
  console.log(`Done. checked_in=${checked}, api_signed=${apiSigned}, already_checked=${already}, dry_run=${dryRuns}, failed=${failed.length}`);

  if (failed.length > 0) {
    console.log('Failed accounts:');
    for (const item of failed) {
      console.log(`- ${item.walletAddress}: ${item.error}`);
    }
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
