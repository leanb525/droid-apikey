// main.ts - Cloudflare Workers Version
interface Env {
  API_KEYS: KVNamespace;
}

// ==================== Type Definitions ====================

interface ApiKey {
  id: string;
  key: string;
}

interface ApiUsageData {
  id: string;
  key: string;
  startDate: string;
  endDate: string;
  orgTotalTokensUsed: number;
  totalAllowance: number;
  usedRatio: number;
}

interface ApiErrorData {
  id: string;
  key: string;
  error: string;
}

type ApiKeyResult = ApiUsageData | ApiErrorData;

interface UsageTotals {
  total_orgTotalTokensUsed: number;
  total_totalAllowance: number;
  totalRemaining: number;
}

interface AggregatedResponse {
  update_time: string;
  total_count: number;
  totals: UsageTotals;
  data: ApiKeyResult[];
}

interface ApiResponse {
  usage: {
    startDate: number;
    endDate: number;
    standard: {
      orgTotalTokensUsed: number;
      totalAllowance: number;
      usedRatio: number;
    };
  };
}

// ==================== Configuration ====================

const CONFIG = {
  API_ENDPOINT: 'https://app.factory.ai/api/organization/members/chat-usage',
  USER_AGENT: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
  TIMEZONE_OFFSET_HOURS: 8,
  KEY_MASK_PREFIX_LENGTH: 4,
  KEY_MASK_SUFFIX_LENGTH: 4,
  AUTO_REFRESH_INTERVAL_SECONDS: 60,
} as const;

// ==================== Database Operations ====================

async function getAllKeys(kv: KVNamespace): Promise<ApiKey[]> {
  const keys: ApiKey[] = [];
  const list = await kv.list({ prefix: "api_keys:" });
  
  for (const key of list.keys) {
    const id = key.name.replace("api_keys:", "");
    const value = await kv.get(key.name);
    if (value) {
      keys.push({ id, key: value });
    }
  }
  
  return keys;
}

async function addKey(kv: KVNamespace, id: string, key: string): Promise<void> {
  await kv.put(`api_keys:${id}`, key);
}

async function deleteKey(kv: KVNamespace, id: string): Promise<void> {
  await kv.delete(`api_keys:${id}`);
}

async function apiKeyExists(kv: KVNamespace, key: string): Promise<boolean> {
  const keys = await getAllKeys(kv);
  return keys.some(k => k.key === key);
}

async function getCachedData(kv: KVNamespace): Promise<AggregatedResponse | null> {
  const cached = await kv.get("cache:data", "json");
  return cached as AggregatedResponse | null;
}

async function setCachedData(kv: KVNamespace, data: AggregatedResponse): Promise<void> {
  await kv.put("cache:data", JSON.stringify(data), {
    expirationTtl: CONFIG.AUTO_REFRESH_INTERVAL_SECONDS * 2
  });
}

// ==================== Utility Functions ====================

function maskApiKey(key: string): string {
  if (key.length <= CONFIG.KEY_MASK_PREFIX_LENGTH + CONFIG.KEY_MASK_SUFFIX_LENGTH) {
    return `${key.substring(0, CONFIG.KEY_MASK_PREFIX_LENGTH)}...`;
  }
  return `${key.substring(0, CONFIG.KEY_MASK_PREFIX_LENGTH)}...${key.substring(key.length - CONFIG.KEY_MASK_SUFFIX_LENGTH)}`;
}

function formatDate(timestamp: number | null | undefined): string {
  if (!timestamp && timestamp !== 0) return 'N/A';
  try {
    return new Date(timestamp).toISOString().split('T')[0];
  } catch {
    return 'Invalid Date';
  }
}

function getBeijingTime(): Date {
  return new Date(Date.now() + CONFIG.TIMEZONE_OFFSET_HOURS * 60 * 60 * 1000);
}

function formatDateTime(date: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function createJsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    },
  });
}

function createErrorResponse(message: string, status = 500): Response {
  return createJsonResponse({ error: message }, status);
}

// ==================== API Data Fetching ====================

async function batchProcess<T, R>(
  items: T[],
  processor: (item: T) => Promise<R>,
  concurrency: number = 10,
  delayMs: number = 100
): Promise<R[]> {
  const results: R[] = [];
  
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map(processor));
    results.push(...batchResults);
    
    if (i + concurrency < items.length) {
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  
  return results;
}

async function fetchApiKeyData(id: string, key: string, retryCount = 0): Promise<ApiKeyResult> {
  const maskedKey = maskApiKey(key);
  const maxRetries = 2;

  try {
    const response = await fetch(CONFIG.API_ENDPOINT, {
      headers: {
        'Authorization': `Bearer ${key}`,
        'User-Agent': CONFIG.USER_AGENT,
      }
    });

    if (!response.ok) {
      if (response.status === 401 && retryCount < maxRetries) {
        const delayMs = (retryCount + 1) * 1000;
        await new Promise(resolve => setTimeout(resolve, delayMs));
        return fetchApiKeyData(id, key, retryCount + 1);
      }
      return { id, key: maskedKey, error: `HTTP ${response.status}` };
    }

    const apiData: ApiResponse = await response.json();
    const { usage } = apiData;
    
    if (!usage?.standard) {
      return { id, key: maskedKey, error: 'Invalid API response' };
    }

    const { standard } = usage;
    return {
      id,
      key: maskedKey,
      startDate: formatDate(usage.startDate),
      endDate: formatDate(usage.endDate),
      orgTotalTokensUsed: standard.orgTotalTokensUsed || 0,
      totalAllowance: standard.totalAllowance || 0,
      usedRatio: standard.usedRatio || 0,
    };
  } catch (error) {
    return { id, key: maskedKey, error: 'Failed to fetch' };
  }
}

const isApiUsageData = (result: ApiKeyResult): result is ApiUsageData => !('error' in result);

async function getAggregatedData(kv: KVNamespace): Promise<AggregatedResponse> {
  const keyPairs = await getAllKeys(kv);
  const beijingTime = getBeijingTime();
  const emptyResponse = {
    update_time: formatDateTime(beijingTime),
    total_count: 0,
    totals: { total_orgTotalTokensUsed: 0, total_totalAllowance: 0, totalRemaining: 0 },
    data: [],
  };

  if (keyPairs.length === 0) return emptyResponse;

  const results = await batchProcess(
    keyPairs,
    ({ id, key }) => fetchApiKeyData(id, key),
    10,
    100
  );

  const validResults = results.filter(isApiUsageData);
  const sortedValid = validResults
    .map(r => ({ ...r, remaining: Math.max(0, r.totalAllowance - r.orgTotalTokensUsed) }))
    .sort((a, b) => b.remaining - a.remaining)
    .map(({ remaining, ...rest }) => rest);

  const totals = validResults.reduce((acc, res) => ({
    total_orgTotalTokensUsed: acc.total_orgTotalTokensUsed + res.orgTotalTokensUsed,
    total_totalAllowance: acc.total_totalAllowance + res.totalAllowance,
    totalRemaining: acc.totalRemaining + Math.max(0, res.totalAllowance - res.orgTotalTokensUsed)
  }), emptyResponse.totals);

  return {
    update_time: formatDateTime(beijingTime),
    total_count: keyPairs.length,
    totals,
    data: [...sortedValid, ...results.filter(r => 'error' in r)],
  };
}

// ==================== Route Handlers ====================

async function handleGetData(kv: KVNamespace): Promise<Response> {
  try {
    // Try to get cached data first
    let cachedData = await getCachedData(kv);
    
    // If no cache, generate new data
    if (!cachedData) {
      cachedData = await getAggregatedData(kv);
      await setCachedData(kv, cachedData);
    }
    
    return createJsonResponse(cachedData);
  } catch (error) {
    return createErrorResponse(error instanceof Error ? error.message : 'Unknown error', 500);
  }
}

async function handleGetKeys(kv: KVNamespace): Promise<Response> {
  try {
    const keys = await getAllKeys(kv);
    return createJsonResponse(keys);
  } catch (error) {
    return createErrorResponse(error instanceof Error ? error.message : 'Unknown error', 500);
  }
}

async function handleAddKeys(req: Request, kv: KVNamespace): Promise<Response> {
  try {
    const body = await req.json();

    if (Array.isArray(body)) {
      return await handleBatchImport(body, kv);
    } else {
      return await handleSingleKeyAdd(body, kv);
    }
  } catch (error) {
    return createErrorResponse(error instanceof Error ? error.message : 'Invalid JSON', 400);
  }
}

async function handleBatchImport(items: any[], kv: KVNamespace): Promise<Response> {
  let added = 0, skipped = 0;
  const existingKeys = new Set((await getAllKeys(kv)).map(k => k.key));

  for (const item of items) {
    if (!item || typeof item !== 'object' || !('key' in item)) continue;
    
    const { key } = item;
    if (!key || existingKeys.has(key)) {
      if (key) skipped++;
      continue;
    }

    const id = `key-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    await addKey(kv, id, key);
    existingKeys.add(key);
    added++;
  }

  if (added > 0) {
    const data = await getAggregatedData(kv);
    await setCachedData(kv, data);
  }

  return createJsonResponse({ success: true, added, skipped });
}

async function handleSingleKeyAdd(body: any, kv: KVNamespace): Promise<Response> {
  if (!body || typeof body !== 'object' || !('key' in body)) {
    return createErrorResponse("key is required", 400);
  }

  const { key } = body;
  if (!key) return createErrorResponse("key cannot be empty", 400);
  if (await apiKeyExists(kv, key)) return createErrorResponse("API key already exists", 409);

  const id = `key-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
  await addKey(kv, id, key);
  
  const data = await getAggregatedData(kv);
  await setCachedData(kv, data);
  
  return createJsonResponse({ success: true });
}

async function handleDeleteKey(pathname: string, kv: KVNamespace): Promise<Response> {
  const id = pathname.split("/api/keys/")[1];
  if (!id) return createErrorResponse("Key ID is required", 400);

  await deleteKey(kv, id);
  
  const data = await getAggregatedData(kv);
  await setCachedData(kv, data);
  
  return createJsonResponse({ success: true });
}

async function handleBatchDeleteKeys(req: Request, kv: KVNamespace): Promise<Response> {
  try {
    const { ids } = await req.json() as { ids: string[] };
    if (!Array.isArray(ids) || ids.length === 0) {
      return createErrorResponse("ids array is required", 400);
    }

    await Promise.all(ids.map(id => deleteKey(kv, id).catch(() => {})));
    
    const data = await getAggregatedData(kv);
    await setCachedData(kv, data);

    return createJsonResponse({ success: true, deleted: ids.length });
  } catch (error) {
    return createErrorResponse(error instanceof Error ? error.message : 'Invalid JSON', 400);
  }
}

async function handleExportKeys(req: Request, kv: KVNamespace): Promise<Response> {
  try {
    const { password } = await req.json() as { password: string };
    const exportPassword = "admin123"; // 硬编码密码

    if (password !== exportPassword) {
      return createErrorResponse("密码错误", 401);
    }

    const keys = await getAllKeys(kv);
    return createJsonResponse({ success: true, keys });
  } catch (error) {
    return createErrorResponse(error instanceof Error ? error.message : 'Invalid JSON', 400);
  }
}

async function handleRefreshSingleKey(pathname: string, kv: KVNamespace): Promise<Response> {
  try {
    const id = pathname.split("/api/keys/")[1].replace("/refresh", "");
    if (!id) return createErrorResponse("Key ID is required", 400);

    const key = await kv.get(`api_keys:${id}`);
    if (!key) return createErrorResponse("Key not found", 404);

    const keyData = await fetchApiKeyData(id, key);
    return createJsonResponse({ success: true, data: keyData });
  } catch (error) {
    return createErrorResponse(error instanceof Error ? error.message : 'Unknown error', 500);
  }
}

// ==================== HTML Content ====================
const HTML_CONTENT = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>API 余额监控看板</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Microsoft YaHei', sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; background: white; border-radius: 16px; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); overflow: hidden; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; position: relative; }
        .header h1 { font-size: 32px; margin-bottom: 10px; }
        .header .update-time { font-size: 14px; opacity: 0.9; }
        .manage-btn { position: absolute; top: 30px; right: 30px; background: rgba(255, 255, 255, 0.2); color: white; border: 2px solid white; border-radius: 8px; padding: 10px 20px; font-size: 14px; cursor: pointer; transition: all 0.3s ease; }
        .manage-btn:hover { background: rgba(255, 255, 255, 0.3); transform: scale(1.05); }
        .stats-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; padding: 30px; background: #f8f9fa; }
        .stat-card { background: white; border-radius: 12px; padding: 20px; text-align: center; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1); transition: transform 0.3s ease, box-shadow 0.3s ease; }
        .stat-card:hover { transform: translateY(-5px); box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15); }
        .stat-card .label { font-size: 13px; color: #6c757d; margin-bottom: 8px; font-weight: 500; }
        .stat-card .value { font-size: 24px; font-weight: bold; color: #667eea; }
        .table-container { padding: 0 30px 30px 30px; overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }
        thead { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        th { padding: 15px; text-align: left; font-weight: 600; font-size: 14px; white-space: nowrap; }
        th.number { text-align: right; }
        td { padding: 12px 15px; border-bottom: 1px solid #e9ecef; font-size: 14px; }
        td.number { text-align: right; font-weight: 500; }
        td.error-row { color: #dc3545; }
        tbody tr:hover { background-color: #f8f9fa; }
        tbody tr:last-child td { border-bottom: none; }
        .key-cell { color: #495057; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .refresh-btn { position: fixed; bottom: 30px; right: 30px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border: none; border-radius: 50px; padding: 15px 30px; font-size: 16px; cursor: pointer; box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4); transition: all 0.3s ease; display: flex; align-items: center; gap: 8px; }
        .refresh-btn:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6); }
        .delete-zero-btn { position: fixed; bottom: 95px; right: 30px; background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); color: white; border: none; border-radius: 50px; padding: 15px 30px; font-size: 16px; cursor: pointer; box-shadow: 0 4px 15px rgba(220, 53, 69, 0.4); transition: all 0.3s ease; }
        .delete-all-btn { position: fixed; bottom: 160px; right: 30px; background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%); color: white; border: none; border-radius: 50px; padding: 15px 30px; font-size: 16px; cursor: pointer; box-shadow: 0 4px 15px rgba(255, 107, 107, 0.4); transition: all 0.3s ease; }
        .export-keys-btn { position: fixed; bottom: 225px; right: 30px; background: linear-gradient(135deg, #28a745 0%, #20c997 100%); color: white; border: none; border-radius: 50px; padding: 15px 30px; font-size: 16px; cursor: pointer; box-shadow: 0 4px 15px rgba(40, 167, 69, 0.4); transition: all 0.3s ease; }
        .loading { text-align: center; padding: 40px; color: #6c757d; }
        .error { text-align: center; padding: 40px; color: #dc3545; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .spinner { display: inline-block; width: 20px; height: 20px; border: 3px solid rgba(255, 255, 255, 0.3); border-radius: 50%; border-top-color: white; animation: spin 1s linear infinite; }
        .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0, 0, 0, 0.5); z-index: 1000; align-items: center; justify-content: center; }
        .modal.show { display: flex; }
        .modal-content { background: white; border-radius: 16px; width: 90%; max-width: 800px; max-height: 90vh; overflow: auto; box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3); }
        .modal-header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px 30px; display: flex; justify-content: space-between; align-items: center; }
        .close-btn { background: none; border: none; color: white; font-size: 28px; cursor: pointer; }
        .modal-body { padding: 30px; }
        .form-group { margin-bottom: 20px; }
        .form-group label { display: block; margin-bottom: 8px; font-weight: 600; }
        .form-group textarea { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 8px; min-height: 150px; font-family: 'Courier New', monospace; }
        .btn { padding: 12px 24px; border: none; border-radius: 8px; font-size: 14px; cursor: pointer; font-weight: 600; }
        .btn-primary { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        .btn-secondary { background: #6c757d; color: white; }
        .btn-danger { background: #dc3545; color: white; }
        .btn-group { display: flex; gap: 10px; margin-top: 20px; }
        .success-msg { background: #d4edda; color: #155724; padding: 12px; border-radius: 8px; margin-bottom: 15px; }
        .error-msg { background: #f8d7da; color: #721c24; padding: 12px; border-radius: 8px; margin-bottom: 15px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <button class="manage-btn" onclick="openManageModal()">Key 管理</button>
            <h1>API 余额监控看板</h1>
            <div class="update-time" id="updateTime">正在加载...</div>
        </div>
        <div class="stats-cards" id="statsCards"></div>
        <div class="table-container">
            <div id="tableContent"><div class="loading">正在加载数据...</div></div>
        </div>
    </div>
    <button class="export-keys-btn" onclick="exportKeys()">📥 导出Key</button>
    <button class="delete-all-btn" onclick="deleteAllKeys()">🗑️ 删除所有</button>
    <button class="delete-zero-btn" onclick="deleteZeroBalanceKeys()">🗑️ 删除无效</button>
    <button class="refresh-btn" onclick="loadData()"><span class="spinner" style="display: none;" id="spinner"></span><span id="btnText">🔄 刷新数据</span></button>
    <div id="manageModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>API Key 管理</h2>
                <button class="close-btn" onclick="closeManageModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div id="modalMessage"></div>
                <form onsubmit="batchImportKeys(event)">
                    <div class="form-group">
                        <label>批量导入 Keys（每行一个 Key）</label>
                        <textarea id="batchKeysInput" placeholder="fk-xxxxx\\nfk-yyyyy"></textarea>
                    </div>
                    <div class="btn-group">
                        <button type="submit" class="btn btn-primary">批量导入</button>
                        <button type="button" class="btn btn-secondary" onclick="document.getElementById('batchKeysInput').value='';">清空</button>
                    </div>
                </form>
            </div>
        </div>
    </div>
    <script>
        let currentApiData = null;
        const formatNumber = (num) => num ? new Intl.NumberFormat('en-US').format(num) : '0';
        const formatPercentage = (ratio) => ratio ? (ratio * 100).toFixed(2) + '%' : '0.00%';
        
        function loadData() {
            document.getElementById('spinner').style.display = 'inline-block';
            document.getElementById('btnText').textContent = '加载中...';
            fetch('/api/data?t=' + Date.now())
                .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
                .then(data => { if (data.error) throw new Error(data.error); displayData(data); })
                .catch(err => document.getElementById('tableContent').innerHTML = \`<div class="error">❌ \${err.message}</div>\`)
                .finally(() => { document.getElementById('spinner').style.display = 'none'; document.getElementById('btnText').textContent = '🔄 刷新数据'; });
        }
        
        function displayData(data) {
            currentApiData = data;
            document.getElementById('updateTime').textContent = \`最后更新: \${data.update_time} | 共 \${data.total_count} 个API Key\`;
            document.getElementById('statsCards').innerHTML = \`
                <div class="stat-card"><div class="label">总计额度</div><div class="value">\${formatNumber(data.totals.total_totalAllowance)}</div></div>
                <div class="stat-card"><div class="label">已使用</div><div class="value">\${formatNumber(data.totals.total_orgTotalTokensUsed)}</div></div>
                <div class="stat-card"><div class="label">剩余额度</div><div class="value">\${formatNumber(data.totals.totalRemaining)}</div></div>
                <div class="stat-card"><div class="label">使用百分比</div><div class="value">\${formatPercentage(data.totals.total_totalAllowance > 0 ? data.totals.total_orgTotalTokensUsed / data.totals.total_totalAllowance : 0)}</div></div>\`;
            
            let html = '<table><thead><tr><th>API Key</th><th>开始时间</th><th>结束时间</th><th class="number">总计额度</th><th class="number">已使用</th><th class="number">剩余额度</th><th class="number">使用百分比</th><th style="text-align:center;">操作</th></tr></thead><tbody>';
            data.data.forEach(item => {
                if (item.error) {
                    html += \`<tr><td class="key-cell" title="\${item.key}">\${item.key}</td><td colspan="5" class="error-row">\${item.error}</td><td style="text-align:center;"><button class="btn btn-primary" onclick="refreshSingleKey('\${item.id}')" style="padding:6px 12px;font-size:12px;margin-right:5px;">刷新</button><button class="btn btn-danger" onclick="deleteKeyFromTable('\${item.id}')" style="padding:6px 12px;font-size:12px;">删除</button></td></tr>\`;
                } else {
                    const remaining = Math.max(0, item.totalAllowance - item.orgTotalTokensUsed);
                    html += \`<tr id="key-row-\${item.id}"><td class="key-cell" title="\${item.key}">\${item.key}</td><td>\${item.startDate}</td><td>\${item.endDate}</td><td class="number">\${formatNumber(item.totalAllowance)}</td><td class="number">\${formatNumber(item.orgTotalTokensUsed)}</td><td class="number">\${formatNumber(remaining)}</td><td class="number">\${formatPercentage(item.usedRatio)}</td><td style="text-align:center;"><button class="btn btn-primary" onclick="refreshSingleKey('\${item.id}')" style="padding:6px 12px;font-size:12px;margin-right:5px;">刷新</button><button class="btn btn-danger" onclick="deleteKeyFromTable('\${item.id}')" style="padding:6px 12px;font-size:12px;">删除</button></td></tr>\`;
                }
            });
            html += '</tbody></table>';
            document.getElementById('tableContent').innerHTML = html;
        }
        
        document.addEventListener('DOMContentLoaded', loadData);
        
        function openManageModal() { document.getElementById('manageModal').classList.add('show'); }
        function closeManageModal() { document.getElementById('manageModal').classList.remove('show'); document.getElementById('modalMessage').innerHTML = ''; }
        function showMessage(msg, isError) { document.getElementById('modalMessage').innerHTML = \`<div class="\${isError ? 'error-msg' : 'success-msg'}">\${msg}</div>\`; setTimeout(() => document.getElementById('modalMessage').innerHTML = '', 5000); }
        
        async function exportKeys() {
            const password = prompt('请输入导出密码：');
            if (!password) return;
            try {
                const r = await fetch('/api/keys/export', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password }) });
                const result = await r.json();
                if (r.ok) {
                    const blob = new Blob([result.keys.map(k => k.key).join('\\n')], { type: 'text/plain' });
                    const a = Object.assign(document.createElement('a'), { href: URL.createObjectURL(blob), download: \`keys_\${new Date().toISOString().split('T')[0]}.txt\` });
                    document.body.appendChild(a); a.click(); document.body.removeChild(a);
                    alert(\`导出 \${result.keys.length} 个Key\`);
                } else alert('导出失败: ' + (result.error || '未知错误'));
            } catch (e) { alert('错误: ' + e.message); }
        }
        
        async function deleteAllKeys() {
            if (!currentApiData || currentApiData.total_count === 0) return alert('没有可删除的Key');
            if (!confirm(\`确定删除所有 \${currentApiData.total_count} 个Key？\`) || prompt('输入 "确认删除"：') !== '确认删除') return;
            try {
                const r = await fetch('/api/keys/batch-delete', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids: currentApiData.data.map(i => i.id) }) });
                const result = await r.json();
                if (r.ok) { alert(\`删除 \${result.deleted} 个Key\`); loadData(); } else alert('失败: ' + (result.error || '未知'));
            } catch (e) { alert('错误: ' + e.message); }
        }
        
        async function deleteZeroBalanceKeys() {
            if (!currentApiData) return alert('请先加载数据');
            const zeros = currentApiData.data.filter(i => !i.error && Math.max(0, (i.totalAllowance || 0) - (i.orgTotalTokensUsed || 0)) === 0);
            if (zeros.length === 0) return alert('没有余额为0的Key');
            if (!confirm(\`删除 \${zeros.length} 个余额为0的Key？\`)) return;
            try {
                const r = await fetch('/api/keys/batch-delete', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids: zeros.map(k => k.id) }) });
                const result = await r.json();
                if (r.ok) { alert(\`删除 \${result.deleted} 个Key\`); loadData(); } else alert('失败: ' + (result.error || '未知'));
            } catch (e) { alert('错误: ' + e.message); }
        }
        
        async function batchImportKeys(e) {
            e.preventDefault();
            const input = document.getElementById('batchKeysInput').value.trim();
            if (!input) return showMessage('请输入Keys', true);
            const lines = input.split('\\n').map(l => l.trim()).filter(l => l);
            const keys = lines.map((line, i) => {
                if (line.includes(':')) {
                    const [id, key] = line.split(':').map(s => s.trim());
                    return id && key ? { id, key } : null;
                }
                return { id: \`key-\${Date.now()}-\${i}-\${Math.floor(Math.random()*1000)}\`, key: line };
            }).filter(k => k);
            if (keys.length === 0) return showMessage('没有有效的Key', true);
            try {
                const r = await fetch('/api/keys', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(keys) });
                const result = await r.json();
                if (r.ok) { showMessage(\`导入 \${result.added} 个Key\${result.skipped > 0 ? \`, 跳过 \${result.skipped} 个\` : ''}\`); document.getElementById('batchKeysInput').value = ''; loadData(); } else showMessage(result.error || '失败', true);
            } catch (e) { showMessage('错误: ' + e.message, true); }
        }
        
        async function deleteKeyFromTable(id) {
            if (!confirm(\`删除 "\${id}"？\`)) return;
            try {
                const r = await fetch(\`/api/keys/\${id}\`, { method: 'DELETE' });
                if (r.ok) { alert('删除成功'); loadData(); } else alert('删除失败');
            } catch (e) { alert('错误: ' + e.message); }
        }
        
        async function refreshSingleKey(id) {
            const row = document.getElementById(\`key-row-\${id}\`);
            if (!row) return;
            const cells = row.querySelectorAll('td');
            const orig = Array.from(cells).map(c => c.innerHTML);
            cells.forEach((c, i) => { if (i > 0 && i < cells.length - 1) c.innerHTML = '<span style="color:#6c757d;">⏳</span>'; });
            try {
                const r = await fetch(\`/api/keys/\${id}/refresh\`, { method: 'POST' });
                const result = await r.json();
                if (r.ok && result.data) {
                    const item = result.data;
                    if (item.error) {
                        cells[1].innerHTML = '<span class="error-row">' + item.error + '</span>';
                        cells[2].colSpan = 5;
                        for (let i = 3; i < cells.length - 1; i++) cells[i].style.display = 'none';
                    } else {
                        const remaining = Math.max(0, item.totalAllowance - item.orgTotalTokensUsed);
                        cells[1].innerHTML = item.startDate;
                        cells[2].innerHTML = item.endDate;
                        cells[3].innerHTML = formatNumber(item.totalAllowance);
                        cells[4].innerHTML = formatNumber(item.orgTotalTokensUsed);
                        cells[5].innerHTML = formatNumber(remaining);
                        cells[6].innerHTML = formatPercentage(item.usedRatio);
                        cells.forEach((c, i) => { if (i > 0 && i < cells.length - 1) { c.style.display = ''; c.colSpan = 1; } });
                    }
                    loadData();
                } else cells.forEach((c, i) => c.innerHTML = orig[i]);
            } catch (e) { cells.forEach((c, i) => c.innerHTML = orig[i]); }
        }
    </script>
</body>
</html>`;

// ==================== Main Worker Handler ====================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    
    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        }
      });
    }

    try {
      // Root - Dashboard
      if (url.pathname === "/") {
        return new Response(HTML_CONTENT, {
          headers: { "Content-Type": "text/html; charset=utf-8" }
        });
      }

      // API Routes
      if (url.pathname === "/api/data" && request.method === "GET") {
        return await handleGetData(env.API_KEYS);
      }

      if (url.pathname === "/api/keys" && request.method === "GET") {
        return await handleGetKeys(env.API_KEYS);
      }

      if (url.pathname === "/api/keys" && request.method === "POST") {
        return await handleAddKeys(request, env.API_KEYS);
      }

      if (url.pathname === "/api/keys/batch-delete" && request.method === "POST") {
        return await handleBatchDeleteKeys(request, env.API_KEYS);
      }

      if (url.pathname === "/api/keys/export" && request.method === "POST") {
        return await handleExportKeys(request, env.API_KEYS);
      }

      if (url.pathname.startsWith("/api/keys/") && request.method === "DELETE") {
        return await handleDeleteKey(url.pathname, env.API_KEYS);
      }

      if (url.pathname.match(/^\/api\/keys\/.+\/refresh$/) && request.method === "POST") {
        return await handleRefreshSingleKey(url.pathname, env.API_KEYS);
      }

      return new Response("Not Found", { status: 404 });
    } catch (error) {
      console.error("Worker error:", error);
      return createErrorResponse(
        error instanceof Error ? error.message : "Internal Server Error",
        500
      );
    }
  }
};
