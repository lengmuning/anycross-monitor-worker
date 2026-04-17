const DEFAULT_BASE_URL = "https://anycross.feishu.cn";

const SCOPE_CONFIG = {
  hosts: {
    path: "/api/agent/v2/monitor/hosts/components/process/status/metrics",
    queryKey: "host",
    batchSize: 10,
    metricName: "host_component_process_status",
  },
  proxy_groups: {
    path: "/api/agent/v2/monitor/proxyGroups/runtime/metrics",
    queryKey: "proxy_group",
    batchSize: 10,
  },
};

const STATUS_CODE_TO_NAME = {
  0: "undefined",
  1: "online",
  2: "unknown",
  3: "offline",
};

const CORS_BASE_HEADERS = {
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Max-Age": "86400",
};

function jsonResponse(payload, status = 200, headers = {}) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...headers,
    },
  });
}

function textResponse(body, status = 200, headers = {}) {
  return new Response(body, {
    status,
    headers,
  });
}

function applyCorsHeaders(headers, request, env) {
  const configuredOrigin = env.ALLOWED_ORIGIN?.trim();
  const requestOrigin = request.headers.get("Origin");
  const allowOrigin = configuredOrigin || requestOrigin || "*";
  const requestHeaders = request.headers.get("Access-Control-Request-Headers");

  headers.set("Access-Control-Allow-Origin", allowOrigin);
  headers.set("Access-Control-Allow-Headers", requestHeaders || "*");

  for (const [key, value] of Object.entries(CORS_BASE_HEADERS)) {
    headers.set(key, value);
  }

  const varyValues = ["Origin", "Access-Control-Request-Headers"];
  const existingVary = headers.get("Vary");
  const varyParts = existingVary
    ? existingVary
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean)
    : [];

  for (const value of varyValues) {
    if (!varyParts.includes(value)) {
      varyParts.push(value);
    }
  }

  headers.set("Vary", varyParts.join(", "));
}

function withCors(response, request, env) {
  const headers = new Headers(response.headers);
  applyCorsHeaders(headers, request, env);

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function getAccessToken(request) {
  const authorization = request.headers.get("Authorization") || "";
  if (authorization.startsWith("Bearer ")) {
    return authorization.slice("Bearer ".length).trim();
  }

  return request.headers.get("X-Access-Token")?.trim() || "";
}

function ensureAuthorized(request, env) {
  const expectedToken = env.ACCESS_TOKEN?.trim();
  if (!expectedToken) {
    return null;
  }

  const providedToken = getAccessToken(request);
  if (providedToken === expectedToken) {
    return null;
  }

  return jsonResponse(
    {
      error: "unauthorized",
      message:
        "Missing or invalid access token. Use Authorization: Bearer <token> or X-Access-Token.",
    },
    401,
    {
      "WWW-Authenticate": 'Bearer realm="anycross-monitor-worker"',
    },
  );
}

function safeJsonParse(input) {
  try {
    return JSON.parse(input);
  } catch {
    return null;
  }
}

function parseListValue(input) {
  if (!input) {
    return [];
  }

  return String(input)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeStringArray(input) {
  if (Array.isArray(input)) {
    return unique(input.flatMap((item) => parseListValue(item)));
  }

  return unique(parseListValue(input));
}

function normalizeObject(input) {
  return input && typeof input === "object" && !Array.isArray(input) ? input : {};
}

function normalizeStringRecord(input) {
  const record = normalizeObject(input);
  const normalized = {};
  for (const [key, value] of Object.entries(record)) {
    if (value === undefined || value === null) {
      continue;
    }
    normalized[key] = value;
  }
  return normalized;
}

function normalizeBoolean(input, defaultValue = false) {
  if (input === undefined || input === null) {
    return defaultValue;
  }

  if (typeof input === "boolean") {
    return input;
  }

  const lowered = String(input).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(lowered)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(lowered)) {
    return false;
  }

  return defaultValue;
}

function unique(items) {
  return [...new Set(items)];
}

function chunkList(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }

  return chunks;
}

function getByPath(value, path) {
  if (!path) {
    return value;
  }

  return String(path)
    .split(".")
    .filter(Boolean)
    .reduce((current, segment) => {
      if (current === null || current === undefined) {
        return undefined;
      }

      if (Array.isArray(current) && /^\d+$/.test(segment)) {
        return current[Number(segment)];
      }

      return current[segment];
    }, value);
}

function interpolateString(template, context) {
  return String(template).replace(/\{\{\s*([^}]+?)\s*\}\}/g, (_, path) => {
    const value = getByPath(context, path);
    if (value === undefined || value === null) {
      return "";
    }
    return String(value);
  });
}

function interpolateValue(value, context) {
  if (Array.isArray(value)) {
    return value.map((item) => interpolateValue(item, context));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, interpolateValue(item, context)]),
    );
  }

  if (typeof value === "string") {
    return interpolateString(value, context);
  }

  return value;
}

function toIsoNow() {
  return new Date().toISOString();
}

function buildLegacyTarget(env) {
  return {
    name: env.DEFAULT_SOURCE_NAME?.trim() || "default",
    base_url: env.ANYCROSS_BASE_URL?.trim() || DEFAULT_BASE_URL,
    api_key: env.ANYCROSS_API_KEY?.trim() || "",
    hosts: normalizeStringArray(env.DEFAULT_HOST_IDS),
    proxy_groups: normalizeStringArray(env.DEFAULT_PROXY_GROUP_IDS),
    vars: {},
    discovery: [],
    version_checks: [],
  };
}

function normalizeDiscoveryConfig(rawConfig, index) {
  return {
    name: String(rawConfig.name || `discovery_${index + 1}`).trim(),
    scope: String(rawConfig.scope || "").trim(),
    path: String(rawConfig.path || "").trim(),
    url: String(rawConfig.url || "").trim(),
    method: String(rawConfig.method || "GET").trim().toUpperCase(),
    auth_mode: String(rawConfig.auth_mode || rawConfig.authMode || "api_key_query").trim(),
    api_key_param: String(rawConfig.api_key_param || rawConfig.apiKeyParam || "api_key").trim(),
    auth_header_name: String(
      rawConfig.auth_header_name || rawConfig.authHeaderName || "X-Api-Key",
    ).trim(),
    auth_value: String(rawConfig.auth_value || rawConfig.authValue || "").trim(),
    query: normalizeStringRecord(rawConfig.query),
    headers: normalizeStringRecord(rawConfig.headers),
    item_path: String(rawConfig.item_path || rawConfig.itemPath || "").trim(),
    id_path: String(rawConfig.id_path || rawConfig.idPath || "").trim(),
    name_path: String(rawConfig.name_path || rawConfig.namePath || "").trim(),
    enabled: normalizeBoolean(rawConfig.enabled, true),
  };
}

function normalizeVersionCheck(rawConfig, index) {
  return {
    name: String(rawConfig.name || `version_check_${index + 1}`).trim(),
    entity_type: String(
      rawConfig.entity_type || rawConfig.entityType || "generic",
    ).trim(),
    path: String(rawConfig.path || "").trim(),
    url: String(rawConfig.url || "").trim(),
    method: String(rawConfig.method || "GET").trim().toUpperCase(),
    auth_mode: String(rawConfig.auth_mode || rawConfig.authMode || "api_key_query").trim(),
    api_key_param: String(rawConfig.api_key_param || rawConfig.apiKeyParam || "api_key").trim(),
    auth_header_name: String(
      rawConfig.auth_header_name || rawConfig.authHeaderName || "X-Api-Key",
    ).trim(),
    auth_value: String(rawConfig.auth_value || rawConfig.authValue || "").trim(),
    query: normalizeStringRecord(rawConfig.query),
    headers: normalizeStringRecord(rawConfig.headers),
    item_path: String(rawConfig.item_path || rawConfig.itemPath || "").trim(),
    id_path: String(rawConfig.id_path || rawConfig.idPath || "").trim(),
    name_path: String(rawConfig.name_path || rawConfig.namePath || "").trim(),
    current_version_path: String(
      rawConfig.current_version_path || rawConfig.currentVersionPath || "",
    ).trim(),
    latest_version_path: String(
      rawConfig.latest_version_path || rawConfig.latestVersionPath || "",
    ).trim(),
    upgrade_available_path: String(
      rawConfig.upgrade_available_path || rawConfig.upgradeAvailablePath || "",
    ).trim(),
    enabled: normalizeBoolean(rawConfig.enabled, true),
  };
}

function normalizeTarget(rawTarget, index, env) {
  const name = String(rawTarget.name || `source_${index + 1}`).trim();
  const baseUrl = String(
    rawTarget.base_url || rawTarget.baseUrl || env.ANYCROSS_BASE_URL || DEFAULT_BASE_URL,
  ).trim();
  const apiKey = String(rawTarget.api_key || rawTarget.apiKey || "").trim();
  const hosts = normalizeStringArray(rawTarget.hosts ?? rawTarget.host_ids ?? rawTarget.hostIds);
  const proxyGroups = normalizeStringArray(
    rawTarget.proxy_groups ?? rawTarget.proxyGroupIds ?? rawTarget.proxyGroups,
  );
  const discovery = Array.isArray(rawTarget.discovery)
    ? rawTarget.discovery.map((config, configIndex) =>
        normalizeDiscoveryConfig(config, configIndex),
      )
    : [];
  const versionChecks = Array.isArray(rawTarget.version_checks || rawTarget.versionChecks)
    ? (rawTarget.version_checks || rawTarget.versionChecks).map((config, configIndex) =>
        normalizeVersionCheck(config, configIndex),
      )
    : [];

  return {
    name,
    base_url: baseUrl || DEFAULT_BASE_URL,
    api_key: apiKey,
    hosts,
    proxy_groups: proxyGroups,
    vars: normalizeStringRecord(rawTarget.vars ?? rawTarget.variables),
    discovery,
    version_checks: versionChecks,
  };
}

function loadTargets(env) {
  const rawTargets = env.ANYCROSS_TARGETS_JSON?.trim();
  if (rawTargets) {
    const parsed = safeJsonParse(rawTargets);
    if (!Array.isArray(parsed)) {
      throw new Error("ANYCROSS_TARGETS_JSON must be a JSON array.");
    }

    return parsed.map((target, index) => normalizeTarget(target, index, env));
  }

  return [buildLegacyTarget(env)];
}

function validateTargets(targets) {
  if (targets.length === 0) {
    return "No AnyCross targets are configured.";
  }

  const names = new Set();
  for (const target of targets) {
    if (!target.name) {
      return "Every AnyCross target must have a non-empty name.";
    }

    if (names.has(target.name)) {
      return `Duplicate target name: ${target.name}`;
    }
    names.add(target.name);

    const hasDiscovery = target.discovery.some((config) => config.enabled);
    const hasVersionChecks = target.version_checks.some((config) => config.enabled);
    const needsApiKey =
      target.hosts.length > 0 ||
      target.proxy_groups.length > 0 ||
      hasDiscovery ||
      hasVersionChecks;

    if (needsApiKey && !target.api_key) {
      const discoveryWithoutApiKey = [...target.discovery, ...target.version_checks].every(
        (config) => config.auth_mode === "none",
      );
      if (!discoveryWithoutApiKey) {
        return `Target "${target.name}" is missing api_key.`;
      }
    }

    if (
      target.hosts.length === 0 &&
      target.proxy_groups.length === 0 &&
      !hasDiscovery &&
      !hasVersionChecks
    ) {
      return `Target "${target.name}" must configure hosts, proxy groups, discovery, or version checks.`;
    }

    for (const config of target.discovery) {
      if (!config.enabled) {
        continue;
      }
      if (!["hosts", "proxy_groups"].includes(config.scope)) {
        return `Target "${target.name}" discovery "${config.name}" must set scope to hosts or proxy_groups.`;
      }
      if (!config.path && !config.url) {
        return `Target "${target.name}" discovery "${config.name}" must set path or url.`;
      }
      if (!config.id_path) {
        return `Target "${target.name}" discovery "${config.name}" must set id_path.`;
      }
    }

    for (const config of target.version_checks) {
      if (!config.enabled) {
        continue;
      }
      if (!config.path && !config.url) {
        return `Target "${target.name}" version check "${config.name}" must set path or url.`;
      }
      if (!config.id_path) {
        return `Target "${target.name}" version check "${config.name}" must set id_path.`;
      }
    }
  }

  return null;
}

function filterTargets(targets, url) {
  const requestedTargets = unique(
    url.searchParams
      .getAll("target")
      .flatMap((value) => parseListValue(value)),
  );

  if (requestedTargets.length === 0) {
    return targets;
  }

  return targets.filter((target) => requestedTargets.includes(target.name));
}

function buildUpstreamUrl(target, scope, ids) {
  const scopeConfig = SCOPE_CONFIG[scope];
  const url = new URL(scopeConfig.path, target.base_url);

  url.searchParams.set("api_key", target.api_key);
  for (const id of ids) {
    url.searchParams.append(scopeConfig.queryKey, id);
  }

  return url;
}

function buildRequestContext(target, config) {
  return {
    target,
    vars: target.vars,
    name: target.name,
    base_url: target.base_url,
    api_key: target.api_key,
    config,
  };
}

function appendQueryParameters(url, query, context) {
  for (const [key, rawValue] of Object.entries(query || {})) {
    const values = Array.isArray(rawValue) ? rawValue : [rawValue];
    for (const value of values) {
      const resolved = interpolateValue(value, context);
      if (resolved === undefined || resolved === null || resolved === "") {
        continue;
      }
      url.searchParams.append(key, String(resolved));
    }
  }
}

function applyAuthHeaders(headers, target, config, context) {
  if (config.auth_mode === "none") {
    return;
  }

  if (config.auth_mode === "api_key_header") {
    headers.set(config.auth_header_name || "X-Api-Key", target.api_key);
    return;
  }

  if (config.auth_mode === "bearer") {
    const token = interpolateString(config.auth_value || "{{api_key}}", context);
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
    return;
  }

  if (config.auth_mode === "header") {
    const value = interpolateString(config.auth_value, context);
    if (value) {
      headers.set(config.auth_header_name || "Authorization", value);
    }
    return;
  }

  if (config.auth_mode === "cookie") {
    const value = interpolateString(config.auth_value, context);
    if (value) {
      headers.set("Cookie", value);
    }
  }
}

function buildJsonApiUrl(target, config) {
  const base = config.url || config.path;
  if (!base) {
    throw new Error(`Request config "${config.name}" is missing path/url.`);
  }

  const url = config.url
    ? new URL(config.url)
    : new URL(config.path, target.base_url);
  const context = buildRequestContext(target, config);

  if (config.auth_mode === "api_key_query") {
    url.searchParams.set(config.api_key_param || "api_key", target.api_key);
  }

  appendQueryParameters(url, config.query, context);

  return url;
}

async function fetchJsonApi(target, config) {
  const url = buildJsonApiUrl(target, config);
  const context = buildRequestContext(target, config);
  const headers = new Headers({
    Accept: "application/json, text/plain;q=0.9, */*;q=0.8",
    "Cache-Control": "no-store",
  });

  for (const [key, value] of Object.entries(config.headers || {})) {
    const resolved = interpolateValue(value, context);
    if (resolved === undefined || resolved === null || resolved === "") {
      continue;
    }
    headers.set(key, String(resolved));
  }
  applyAuthHeaders(headers, target, config, context);

  const response = await fetch(url.toString(), {
    method: config.method || "GET",
    headers,
    cf: {
      cacheTtl: 0,
      cacheEverything: false,
    },
  });

  const body = await response.text();
  const parsed = safeJsonParse(body);

  return {
    url,
    response,
    body,
    parsed,
  };
}

function mergeMetricFamilies(targetFamilies, sourceFamilies) {
  for (const [metricName, metadata] of sourceFamilies.entries()) {
    const current = targetFamilies.get(metricName) || {};
    targetFamilies.set(metricName, {
      help: current.help || metadata.help || "",
      type: current.type || metadata.type || "",
    });
  }
}

function splitLabelPairs(raw) {
  const pairs = [];
  let current = "";
  let inQuotes = false;
  let escaping = false;

  for (const char of raw) {
    if (escaping) {
      current += char;
      escaping = false;
      continue;
    }

    if (char === "\\") {
      current += char;
      escaping = true;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      current += char;
      continue;
    }

    if (char === "," && !inQuotes) {
      pairs.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  if (current) {
    pairs.push(current);
  }

  return pairs.filter(Boolean);
}

function unescapeLabelValue(value) {
  return value.replace(/\\\\/g, "\\").replace(/\\"/g, '"').replace(/\\n/g, "\n");
}

function escapeLabelValue(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}

function escapeHelpValue(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/\n/g, "\\n");
}

function parseLabels(rawLabels = "") {
  if (!rawLabels) {
    return {};
  }

  const labels = {};
  for (const pair of splitLabelPairs(rawLabels)) {
    const separatorIndex = pair.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }

    const key = pair.slice(0, separatorIndex).trim();
    const rawValue = pair.slice(separatorIndex + 1).trim();
    const trimmedValue =
      rawValue.startsWith('"') && rawValue.endsWith('"')
        ? rawValue.slice(1, -1)
        : rawValue;

    labels[key] = unescapeLabelValue(trimmedValue);
  }

  return labels;
}

function parseMetricValue(rawValue) {
  if (rawValue === "+Inf") {
    return Infinity;
  }
  if (rawValue === "-Inf") {
    return -Infinity;
  }
  if (rawValue === "NaN") {
    return NaN;
  }
  return Number(rawValue);
}

function parseOpenMetricsDocument(document) {
  const samples = [];
  const metricFamilies = new Map();
  const lines = document.split(/\r?\n/);

  for (const line of lines) {
    const trimmedLine = line.trim();
    if (!trimmedLine) {
      continue;
    }

    if (trimmedLine.startsWith("# HELP ")) {
      const match = trimmedLine.match(/^# HELP ([^\s]+)\s+(.+)$/);
      if (match) {
        const [, metricName, help] = match;
        const current = metricFamilies.get(metricName) || {};
        metricFamilies.set(metricName, {
          ...current,
          help,
        });
      }
      continue;
    }

    if (trimmedLine.startsWith("# TYPE ")) {
      const match = trimmedLine.match(/^# TYPE ([^\s]+)\s+([^\s]+)$/);
      if (match) {
        const [, metricName, type] = match;
        const current = metricFamilies.get(metricName) || {};
        metricFamilies.set(metricName, {
          ...current,
          type,
        });
      }
      continue;
    }

    if (trimmedLine.startsWith("#")) {
      continue;
    }

    const match = trimmedLine.match(
      /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)(?:\s+\S+)?$/,
    );
    if (!match) {
      continue;
    }

    const [, name, rawLabels = "", rawValue] = match;
    samples.push({
      name,
      labels: parseLabels(rawLabels),
      value: parseMetricValue(rawValue),
      rawValue,
    });
  }

  return {
    samples,
    metricFamilies,
  };
}

function formatMetricValue(sample) {
  if (sample.rawValue) {
    return sample.rawValue;
  }
  if (sample.value === Infinity) {
    return "+Inf";
  }
  if (sample.value === -Infinity) {
    return "-Inf";
  }
  if (Number.isNaN(sample.value)) {
    return "NaN";
  }
  return String(sample.value);
}

function renderSample(sample) {
  const labels = Object.entries(sample.labels || {})
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .sort(([left], [right]) => left.localeCompare(right));

  const labelText =
    labels.length === 0
      ? ""
      : `{${labels
          .map(([key, value]) => `${key}="${escapeLabelValue(value)}"`)
          .join(",")}}`;

  return `${sample.name}${labelText} ${formatMetricValue(sample)}`;
}

async function fetchScopeBatch(target, scope, ids) {
  const upstreamUrl = buildUpstreamUrl(target, scope, ids);
  const response = await fetch(upstreamUrl.toString(), {
    method: "GET",
    headers: {
      Accept: "application/openmetrics-text; version=1.0.0, text/plain;q=0.9, */*;q=0.8",
      "Cache-Control": "no-store",
    },
    cf: {
      cacheTtl: 0,
      cacheEverything: false,
    },
  });

  return {
    upstreamUrl,
    response,
    body: await response.text(),
  };
}

async function collectScope(target, scope) {
  const ids = target[scope];
  const scopeConfig = SCOPE_CONFIG[scope];
  const startedAt = Date.now();

  if (ids.length === 0) {
    return {
      scope,
      configured: false,
      requested_ids: [],
      batches: [],
      samples: [],
      metric_families: new Map(),
      errors: [],
      duration_ms: 0,
      success: false,
    };
  }

  const metricFamilies = new Map();
  const samples = [];
  const errors = [];
  const batches = [];

  for (const batchIds of chunkList(ids, scopeConfig.batchSize)) {
    const batchStartedAt = Date.now();
    try {
      const { upstreamUrl, response, body } = await fetchScopeBatch(target, scope, batchIds);
      const batchSummary = {
        ids: batchIds,
        upstream_url: upstreamUrl.toString(),
        duration_ms: Date.now() - batchStartedAt,
        upstream_status: response.status,
        success: response.ok,
      };

      if (!response.ok) {
        const contentType = response.headers.get("content-type") || "";
        const errorBody = contentType.includes("application/json")
          ? safeJsonParse(body) || body
          : body;
        batches.push(batchSummary);
        errors.push({
          scope,
          ids: batchIds,
          upstream_url: upstreamUrl.toString(),
          upstream_status: response.status,
          upstream_status_text: response.statusText,
          upstream_body: errorBody,
        });
        continue;
      }

      const parsed = parseOpenMetricsDocument(body);
      for (const sample of parsed.samples) {
        samples.push({
          ...sample,
          labels: {
            ...sample.labels,
            worker_source: target.name,
          },
        });
      }
      mergeMetricFamilies(metricFamilies, parsed.metricFamilies);
      batches.push(batchSummary);
    } catch (error) {
      errors.push({
        scope,
        ids: batchIds,
        upstream_url: buildUpstreamUrl(target, scope, batchIds).toString(),
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return {
    scope,
    configured: true,
    requested_ids: ids,
    batches,
    samples,
    metric_families: metricFamilies,
    errors,
    duration_ms: Date.now() - startedAt,
    success: errors.length === 0,
  };
}

function extractItemsFromJson(payload, itemPath) {
  const extracted = itemPath ? getByPath(payload, itemPath) : payload;
  if (Array.isArray(extracted)) {
    return extracted;
  }
  if (extracted && Array.isArray(extracted.items)) {
    return extracted.items;
  }
  if (extracted && Array.isArray(extracted.list)) {
    return extracted.list;
  }
  return [];
}

async function collectDiscoveryScope(target, scope) {
  const configs = target.discovery.filter((config) => config.enabled && config.scope === scope);
  const staticIds = target[scope];

  if (configs.length === 0) {
    return {
      configured: false,
      static_ids: staticIds,
      discovered_ids: [],
      effective_ids: staticIds,
      endpoints: [],
      errors: [],
      used_discovery: false,
      fetched_at: toIsoNow(),
    };
  }

  const endpoints = [];
  const errors = [];
  const discoveredIds = [];

  for (const config of configs) {
    const startedAt = Date.now();
    try {
      const { url, response, body, parsed } = await fetchJsonApi(target, config);
      const endpoint = {
        name: config.name,
        scope,
        upstream_url: url.toString(),
        upstream_status: response.status,
        duration_ms: Date.now() - startedAt,
        success: response.ok,
      };

      if (!response.ok) {
        const contentType = response.headers.get("content-type") || "";
        const errorBody = contentType.includes("application/json")
          ? parsed || body
          : body;
        endpoints.push(endpoint);
        errors.push({
          name: config.name,
          scope,
          upstream_url: url.toString(),
          upstream_status: response.status,
          upstream_status_text: response.statusText,
          upstream_body: errorBody,
        });
        continue;
      }

      if (!parsed || typeof parsed !== "object") {
        endpoints.push(endpoint);
        errors.push({
          name: config.name,
          scope,
          upstream_url: url.toString(),
          message: "Discovery endpoint did not return valid JSON.",
        });
        continue;
      }

      const items = extractItemsFromJson(parsed, config.item_path);
      const ids = unique(
        items
          .map((item) => getByPath(item, config.id_path))
          .filter((value) => value !== undefined && value !== null && String(value).trim() !== "")
          .map((value) => String(value).trim()),
      );

      endpoints.push({
        ...endpoint,
        item_count: items.length,
        discovered_count: ids.length,
      });
      discoveredIds.push(...ids);
    } catch (error) {
      errors.push({
        name: config.name,
        scope,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const uniqueDiscoveredIds = unique(discoveredIds);
  return {
    configured: true,
    static_ids: staticIds,
    discovered_ids: uniqueDiscoveredIds,
    effective_ids: uniqueDiscoveredIds.length > 0 ? uniqueDiscoveredIds : staticIds,
    endpoints,
    errors,
    used_discovery: uniqueDiscoveredIds.length > 0,
    fetched_at: toIsoNow(),
  };
}

function normalizeVersionFlag(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (value === undefined || value === null) {
    return null;
  }

  const lowered = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(lowered)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(lowered)) {
    return false;
  }

  return null;
}

function computeUpgradeAvailable(item, config) {
  const explicit = config.upgrade_available_path
    ? normalizeVersionFlag(getByPath(item, config.upgrade_available_path))
    : null;
  if (explicit !== null) {
    return explicit;
  }

  const currentVersion = config.current_version_path
    ? getByPath(item, config.current_version_path)
    : undefined;
  const latestVersion = config.latest_version_path
    ? getByPath(item, config.latest_version_path)
    : undefined;

  if (latestVersion === undefined || latestVersion === null || latestVersion === "") {
    return false;
  }
  if (currentVersion === undefined || currentVersion === null || currentVersion === "") {
    return true;
  }

  return String(currentVersion).trim() !== String(latestVersion).trim();
}

async function collectVersionChecks(target) {
  const configs = target.version_checks.filter((config) => config.enabled);
  if (configs.length === 0) {
    return {
      configured: false,
      checks: [],
      totals: {
        check_count: 0,
        item_count: 0,
        upgrade_available_count: 0,
      },
      errors: [],
      fetched_at: toIsoNow(),
    };
  }

  const checks = [];
  const errors = [];

  for (const config of configs) {
    const startedAt = Date.now();
    try {
      const { url, response, body, parsed } = await fetchJsonApi(target, config);
      const summary = {
        name: config.name,
        entity_type: config.entity_type,
        upstream_url: url.toString(),
        upstream_status: response.status,
        duration_ms: Date.now() - startedAt,
        success: response.ok,
        items: [],
        errors: [],
      };

      if (!response.ok) {
        const contentType = response.headers.get("content-type") || "";
        const errorBody = contentType.includes("application/json")
          ? parsed || body
          : body;
        summary.errors.push({
          upstream_status: response.status,
          upstream_status_text: response.statusText,
          upstream_body: errorBody,
        });
        checks.push(summary);
        errors.push({
          name: config.name,
          message: `Version endpoint returned status ${response.status}.`,
        });
        continue;
      }

      if (!parsed || typeof parsed !== "object") {
        summary.errors.push({
          message: "Version endpoint did not return valid JSON.",
        });
        checks.push(summary);
        errors.push({
          name: config.name,
          message: "Version endpoint did not return valid JSON.",
        });
        continue;
      }

      const items = extractItemsFromJson(parsed, config.item_path);
      summary.items = items
        .map((item) => {
          const entityId = getByPath(item, config.id_path);
          if (entityId === undefined || entityId === null || String(entityId).trim() === "") {
            return null;
          }

          const entityName = config.name_path ? getByPath(item, config.name_path) : entityId;
          const currentVersion = config.current_version_path
            ? getByPath(item, config.current_version_path)
            : null;
          const latestVersion = config.latest_version_path
            ? getByPath(item, config.latest_version_path)
            : null;
          const upgradeAvailable = computeUpgradeAvailable(item, config);

          return {
            check_name: config.name,
            entity_type: config.entity_type,
            entity_id: String(entityId).trim(),
            entity_name:
              entityName === undefined || entityName === null || entityName === ""
                ? String(entityId).trim()
                : String(entityName).trim(),
            current_version:
              currentVersion === undefined || currentVersion === null
                ? null
                : String(currentVersion).trim(),
            latest_version:
              latestVersion === undefined || latestVersion === null
                ? null
                : String(latestVersion).trim(),
            upgrade_available: upgradeAvailable,
            raw: item,
          };
        })
        .filter(Boolean);
      checks.push(summary);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      checks.push({
        name: config.name,
        entity_type: config.entity_type,
        upstream_url: config.url || config.path,
        upstream_status: null,
        duration_ms: Date.now() - startedAt,
        success: false,
        items: [],
        errors: [{ message }],
      });
      errors.push({
        name: config.name,
        message,
      });
    }
  }

  const allItems = checks.flatMap((check) => check.items);
  return {
    configured: true,
    checks,
    totals: {
      check_count: checks.length,
      item_count: allItems.length,
      upgrade_available_count: allItems.filter((item) => item.upgrade_available).length,
    },
    errors,
    fetched_at: toIsoNow(),
  };
}

async function collectTarget(target) {
  const [hostDiscovery, proxyGroupDiscovery, versions] = await Promise.all([
    collectDiscoveryScope(target, "hosts"),
    collectDiscoveryScope(target, "proxy_groups"),
    collectVersionChecks(target),
  ]);

  const effectiveTarget = {
    ...target,
    hosts: hostDiscovery.effective_ids,
    proxy_groups: proxyGroupDiscovery.effective_ids,
  };

  const [hosts, proxyGroups] = await Promise.all([
    collectScope(effectiveTarget, "hosts"),
    collectScope(effectiveTarget, "proxy_groups"),
  ]);

  return {
    source: target.name,
    base_url: target.base_url,
    discovery: {
      hosts: hostDiscovery,
      proxy_groups: proxyGroupDiscovery,
    },
    hosts,
    proxy_groups: proxyGroups,
    versions,
  };
}

function normalizeStatus(sample) {
  if (sample.labels.status) {
    return sample.labels.status;
  }

  const numericValue = Number(sample.value);
  if (Number.isFinite(numericValue)) {
    return STATUS_CODE_TO_NAME[numericValue] || "unknown";
  }

  return "unknown";
}

function summarizeHostStatus(components) {
  const counts = {
    undefined: 0,
    online: 0,
    unknown: 0,
    offline: 0,
  };

  for (const component of components) {
    if (counts[component.status] !== undefined) {
      counts[component.status] += 1;
    }
  }

  let overallStatus = "undefined";
  if (counts.offline > 0) {
    overallStatus = "offline";
  } else if (counts.unknown > 0) {
    overallStatus = "unknown";
  } else if (counts.online > 0) {
    overallStatus = "online";
  }

  return {
    overall_status: overallStatus,
    counts,
  };
}

function buildHostSummary(scopeResult) {
  const samples = scopeResult.samples.filter(
    (sample) => sample.name === SCOPE_CONFIG.hosts.metricName,
  );

  const hosts = new Map();
  for (const sample of samples) {
    const hostId = sample.labels.host_id || "unknown";
    const hostName = sample.labels.host_name || hostId;
    const host = hosts.get(hostId) || {
      host_id: hostId,
      host_name: hostName,
      components: [],
    };

    host.components.push({
      metric: sample.name,
      component_id: sample.labels.component_id || "",
      component_type: sample.labels.component_type || "",
      host_id: hostId,
      host_name: hostName,
      status: normalizeStatus(sample),
      status_code: Number.isFinite(sample.value) ? Number(sample.value) : null,
      raw_value: sample.rawValue,
    });

    hosts.set(hostId, host);
  }

  const hostList = [...hosts.values()]
    .map((host) => ({
      ...host,
      summary: summarizeHostStatus(host.components),
    }))
    .sort((left, right) => left.host_id.localeCompare(right.host_id));

  const totals = {
    requested_hosts: scopeResult.requested_ids.length,
    returned_hosts: hostList.length,
    returned_components: hostList.reduce((count, host) => count + host.components.length, 0),
    status_counts: {
      undefined: 0,
      online: 0,
      unknown: 0,
      offline: 0,
    },
  };

  for (const host of hostList) {
    for (const [status, count] of Object.entries(host.summary.counts)) {
      totals.status_counts[status] += count;
    }
  }

  return {
    configured: scopeResult.configured,
    requested_host_ids: scopeResult.requested_ids,
    hosts: hostList,
    totals,
    errors: scopeResult.errors,
  };
}

function buildProxyGroupSummary(scopeResult) {
  const groups = new Map();

  for (const sample of scopeResult.samples) {
    const proxyGroupId = sample.labels.proxy_group_id || "unknown";
    const group = groups.get(proxyGroupId) || {
      proxy_group_id: proxyGroupId,
      samples: [],
      metric_names: new Set(),
    };

    group.samples.push({
      metric: sample.name,
      labels: sample.labels,
      value: Number.isFinite(sample.value) ? Number(sample.value) : sample.rawValue,
      raw_value: sample.rawValue,
    });
    group.metric_names.add(sample.name);
    groups.set(proxyGroupId, group);
  }

  const proxyGroups = [...groups.values()]
    .map((group) => ({
      proxy_group_id: group.proxy_group_id,
      sample_count: group.samples.length,
      metric_names: [...group.metric_names].sort(),
      samples: group.samples,
    }))
    .sort((left, right) => left.proxy_group_id.localeCompare(right.proxy_group_id));

  return {
    configured: scopeResult.configured,
    requested_proxy_group_ids: scopeResult.requested_ids,
    proxy_groups: proxyGroups,
    totals: {
      requested_proxy_groups: scopeResult.requested_ids.length,
      returned_proxy_groups: proxyGroups.length,
      returned_samples: scopeResult.samples.length,
      metric_names: unique(scopeResult.samples.map((sample) => sample.name)).sort(),
    },
    errors: scopeResult.errors,
  };
}

function buildDiscoveryScopeSummary(scopeResult) {
  return {
    configured: scopeResult.configured,
    used_discovery: scopeResult.used_discovery,
    static_ids: scopeResult.static_ids,
    discovered_ids: scopeResult.discovered_ids,
    effective_ids: scopeResult.effective_ids,
    endpoints: scopeResult.endpoints,
    errors: scopeResult.errors,
    fetched_at: scopeResult.fetched_at,
  };
}

function buildVersionSummary(versionResult) {
  return {
    configured: versionResult.configured,
    fetched_at: versionResult.fetched_at,
    totals: versionResult.totals,
    errors: versionResult.errors,
    checks: versionResult.checks.map((check) => ({
      name: check.name,
      entity_type: check.entity_type,
      upstream_url: check.upstream_url,
      upstream_status: check.upstream_status,
      duration_ms: check.duration_ms,
      success: check.success,
      item_count: check.items.length,
      upgrade_available_count: check.items.filter((item) => item.upgrade_available).length,
      items: check.items.map((item) => ({
        check_name: item.check_name,
        entity_type: item.entity_type,
        entity_id: item.entity_id,
        entity_name: item.entity_name,
        current_version: item.current_version,
        latest_version: item.latest_version,
        upgrade_available: item.upgrade_available,
      })),
      errors: check.errors,
    })),
  };
}

function buildStatusPayload(results) {
  return {
    fetched_at: toIsoNow(),
    sources: results.map((result) => ({
      source: result.source,
      base_url: result.base_url,
      discovery: {
        hosts: buildDiscoveryScopeSummary(result.discovery.hosts),
        proxy_groups: buildDiscoveryScopeSummary(result.discovery.proxy_groups),
      },
      scopes: {
        hosts: buildHostSummary(result.hosts),
        proxy_groups: buildProxyGroupSummary(result.proxy_groups),
      },
      versions: buildVersionSummary(result.versions),
    })),
  };
}

function buildDiscoveryPayload(results) {
  return {
    fetched_at: toIsoNow(),
    sources: results.map((result) => ({
      source: result.source,
      base_url: result.base_url,
      discovery: {
        hosts: buildDiscoveryScopeSummary(result.discovery.hosts),
        proxy_groups: buildDiscoveryScopeSummary(result.discovery.proxy_groups),
      },
    })),
  };
}

function buildVersionsPayload(results) {
  return {
    fetched_at: toIsoNow(),
    sources: results.map((result) => ({
      source: result.source,
      base_url: result.base_url,
      versions: buildVersionSummary(result.versions),
    })),
  };
}

function normalizeAlertRule(rawRule, index, env) {
  return {
    name: String(rawRule.name || `rule_${index + 1}`).trim(),
    type: String(rawRule.type || "").trim(),
    severity: String(rawRule.severity || "warning").trim(),
    cooldown_seconds: Number(rawRule.cooldown_seconds ?? rawRule.cooldownSeconds ?? 900),
    notify_resolved: rawRule.notify_resolved ?? rawRule.notifyResolved ?? true,
    webhook_url: String(
      rawRule.webhook_url || rawRule.webhookUrl || env.FEISHU_WEBHOOK_URL || "",
    ).trim(),
    webhook_secret: String(
      rawRule.webhook_secret || rawRule.webhookSecret || env.FEISHU_WEBHOOK_SECRET || "",
    ).trim(),
    sources: normalizeStringArray(rawRule.sources),
    host_ids: normalizeStringArray(rawRule.host_ids ?? rawRule.hostIds),
    component_ids: normalizeStringArray(rawRule.component_ids ?? rawRule.componentIds),
    component_types: normalizeStringArray(rawRule.component_types ?? rawRule.componentTypes),
    statuses: normalizeStringArray(rawRule.statuses),
    proxy_group_ids: normalizeStringArray(rawRule.proxy_group_ids ?? rawRule.proxyGroupIds),
    entity_ids: normalizeStringArray(rawRule.entity_ids ?? rawRule.entityIds),
    entity_types: normalizeStringArray(rawRule.entity_types ?? rawRule.entityTypes),
    check_names: normalizeStringArray(rawRule.check_names ?? rawRule.checkNames),
    metric: String(rawRule.metric || "").trim(),
    op: String(rawRule.op || rawRule.operator || "").trim(),
    threshold:
      rawRule.threshold === undefined || rawRule.threshold === null
        ? null
        : Number(rawRule.threshold),
    aggregate: String(rawRule.aggregate || "max").trim(),
    group_by: normalizeStringArray(rawRule.group_by ?? rawRule.groupBy),
    label_filters: rawRule.label_filters ?? rawRule.labelFilters ?? {},
  };
}

function loadAlertRules(env) {
  const rawRules = env.ANYCROSS_ALERTS_JSON?.trim();
  if (!rawRules) {
    return [];
  }

  const parsed = safeJsonParse(rawRules);
  if (!Array.isArray(parsed)) {
    throw new Error("ANYCROSS_ALERTS_JSON must be a JSON array.");
  }

  return parsed.map((rule, index) => normalizeAlertRule(rule, index, env));
}

function validateAlertRules(rules) {
  const names = new Set();
  for (const rule of rules) {
    if (!rule.name) {
      return "Every alert rule must have a non-empty name.";
    }

    if (names.has(rule.name)) {
      return `Duplicate alert rule name: ${rule.name}`;
    }
    names.add(rule.name);

    if (!rule.webhook_url) {
      return `Alert rule "${rule.name}" is missing webhook_url.`;
    }

    if (
      ![
        "host_status",
        "host_component_status",
        "proxy_group_metric",
        "version_upgrade_available",
      ].includes(rule.type)
    ) {
      return `Alert rule "${rule.name}" has unsupported type "${rule.type}".`;
    }

    if (!Number.isFinite(rule.cooldown_seconds) || rule.cooldown_seconds < 0) {
      return `Alert rule "${rule.name}" has invalid cooldown_seconds.`;
    }

    if (rule.type === "proxy_group_metric") {
      if (!rule.metric) {
        return `Alert rule "${rule.name}" must define metric.`;
      }

      if (!["<", "<=", ">", ">=", "==", "!="].includes(rule.op)) {
        return `Alert rule "${rule.name}" has unsupported op "${rule.op}".`;
      }

      if (!Number.isFinite(rule.threshold)) {
        return `Alert rule "${rule.name}" must define a numeric threshold.`;
      }

      if (!["max", "min", "avg", "sum"].includes(rule.aggregate)) {
        return `Alert rule "${rule.name}" has unsupported aggregate "${rule.aggregate}".`;
      }
    }
  }

  return null;
}

function matchesOptionalFilter(values, targetValue) {
  if (!values || values.length === 0) {
    return true;
  }

  return values.includes(targetValue);
}

function matchesLabelFilters(labels, labelFilters) {
  return Object.entries(labelFilters || {}).every(([key, expected]) => {
    if (Array.isArray(expected)) {
      return expected.map(String).includes(String(labels[key] || ""));
    }

    return String(labels[key] || "") === String(expected);
  });
}

function toRuleStatuses(rule) {
  return rule.statuses.length > 0 ? rule.statuses : ["offline", "unknown"];
}

function buildAlertFingerprint(parts) {
  return parts.map((part) => encodeURIComponent(String(part))).join(":");
}

function evaluateHostStatusRule(rule, statusPayload) {
  const statuses = toRuleStatuses(rule);
  const events = [];

  for (const source of statusPayload.sources) {
    if (!matchesOptionalFilter(rule.sources, source.source)) {
      continue;
    }

    for (const host of source.scopes.hosts.hosts) {
      if (!matchesOptionalFilter(rule.host_ids, host.host_id)) {
        continue;
      }

      if (!statuses.includes(host.summary.overall_status)) {
        continue;
      }

      const fingerprint = buildAlertFingerprint([
        rule.name,
        source.source,
        host.host_id,
        host.summary.overall_status,
      ]);

      events.push({
        rule_name: rule.name,
        rule_type: rule.type,
        severity: rule.severity,
        fingerprint,
        title: `[${rule.severity.toUpperCase()}] 宿主机状态异常`,
        summary: `${source.source}/${host.host_name} 当前状态 ${host.summary.overall_status}`,
        detail: [
          `规则: ${rule.name}`,
          `来源: ${source.source}`,
          `宿主机: ${host.host_name} (${host.host_id})`,
          `状态: ${host.summary.overall_status}`,
          `组件计数: online=${host.summary.counts.online}, unknown=${host.summary.counts.unknown}, offline=${host.summary.counts.offline}`,
        ].join("\n"),
        labels: {
          worker_source: source.source,
          host_id: host.host_id,
          host_name: host.host_name,
          status: host.summary.overall_status,
        },
        value: host.summary.overall_status,
      });
    }
  }

  return events;
}

function evaluateHostComponentStatusRule(rule, statusPayload) {
  const statuses = toRuleStatuses(rule);
  const events = [];

  for (const source of statusPayload.sources) {
    if (!matchesOptionalFilter(rule.sources, source.source)) {
      continue;
    }

    for (const host of source.scopes.hosts.hosts) {
      if (!matchesOptionalFilter(rule.host_ids, host.host_id)) {
        continue;
      }

      for (const component of host.components) {
        if (!matchesOptionalFilter(rule.component_ids, component.component_id)) {
          continue;
        }

        if (!matchesOptionalFilter(rule.component_types, component.component_type)) {
          continue;
        }

        if (!statuses.includes(component.status)) {
          continue;
        }

        const fingerprint = buildAlertFingerprint([
          rule.name,
          source.source,
          component.host_id,
          component.component_id,
          component.status,
        ]);

        events.push({
          rule_name: rule.name,
          rule_type: rule.type,
          severity: rule.severity,
          fingerprint,
          title: `[${rule.severity.toUpperCase()}] 宿主机组件状态异常`,
          summary: `${source.source}/${component.host_name}/${component.component_id} 状态 ${component.status}`,
          detail: [
            `规则: ${rule.name}`,
            `来源: ${source.source}`,
            `宿主机: ${component.host_name} (${component.host_id})`,
            `组件: ${component.component_id}`,
            `组件类型: ${component.component_type}`,
            `状态: ${component.status}`,
            `状态码: ${component.status_code}`,
          ].join("\n"),
          labels: {
            worker_source: source.source,
            host_id: component.host_id,
            host_name: component.host_name,
            component_id: component.component_id,
            component_type: component.component_type,
            status: component.status,
          },
          value: component.status_code,
        });
      }
    }
  }

  return events;
}

function aggregateValues(values, aggregate) {
  if (values.length === 0) {
    return null;
  }
  if (aggregate === "max") {
    return Math.max(...values);
  }
  if (aggregate === "min") {
    return Math.min(...values);
  }
  if (aggregate === "sum") {
    return values.reduce((sum, value) => sum + value, 0);
  }
  if (aggregate === "avg") {
    return values.reduce((sum, value) => sum + value, 0) / values.length;
  }
  return null;
}

function compareValues(actual, op, threshold) {
  if (op === ">") return actual > threshold;
  if (op === ">=") return actual >= threshold;
  if (op === "<") return actual < threshold;
  if (op === "<=") return actual <= threshold;
  if (op === "==") return actual === threshold;
  if (op === "!=") return actual !== threshold;
  return false;
}

function evaluateProxyGroupMetricRule(rule, statusPayload) {
  const events = [];
  const groupBy = rule.group_by.length > 0 ? rule.group_by : ["worker_source", "proxy_group_id"];

  for (const source of statusPayload.sources) {
    if (!matchesOptionalFilter(rule.sources, source.source)) {
      continue;
    }

    const groups = new Map();
    for (const proxyGroup of source.scopes.proxy_groups.proxy_groups) {
      if (!matchesOptionalFilter(rule.proxy_group_ids, proxyGroup.proxy_group_id)) {
        continue;
      }

      for (const sample of proxyGroup.samples) {
        if (sample.metric !== rule.metric) {
          continue;
        }
        if (!matchesLabelFilters(sample.labels, rule.label_filters)) {
          continue;
        }

        const labels = {
          ...sample.labels,
          worker_source: source.source,
        };
        const groupValues = groupBy.map((field) => labels[field] || "");
        const groupKey = buildAlertFingerprint([rule.name, ...groupValues]);
        const current = groups.get(groupKey) || {
          fingerprint: groupKey,
          labels,
          samples: [],
        };
        current.samples.push(sample);
        groups.set(groupKey, current);
      }
    }

    for (const group of groups.values()) {
      const values = group.samples
        .map((sample) => Number(sample.value))
        .filter((value) => Number.isFinite(value));
      const aggregatedValue = aggregateValues(values, rule.aggregate);
      if (aggregatedValue === null) {
        continue;
      }
      if (!compareValues(aggregatedValue, rule.op, rule.threshold)) {
        continue;
      }

      const proxyGroupId = group.labels.proxy_group_id || "unknown";
      events.push({
        rule_name: rule.name,
        rule_type: rule.type,
        severity: rule.severity,
        fingerprint: group.fingerprint,
        title: `[${rule.severity.toUpperCase()}] 代理集群指标异常`,
        summary:
          `${source.source}/${proxyGroupId} ${rule.metric} ${rule.aggregate}=${aggregatedValue} ${rule.op} ${rule.threshold}`,
        detail: [
          `规则: ${rule.name}`,
          `来源: ${source.source}`,
          `代理集群: ${proxyGroupId}`,
          `指标: ${rule.metric}`,
          `聚合: ${rule.aggregate}`,
          `比较: ${aggregatedValue} ${rule.op} ${rule.threshold}`,
          `标签: ${Object.entries(group.labels)
            .map(([key, value]) => `${key}=${value}`)
            .join(", ")}`,
          `样本数: ${group.samples.length}`,
        ].join("\n"),
        labels: {
          ...group.labels,
          proxy_group_id: proxyGroupId,
        },
        value: aggregatedValue,
      });
    }
  }

  return events;
}

function evaluateVersionUpgradeRule(rule, statusPayload) {
  const events = [];

  for (const source of statusPayload.sources) {
    if (!matchesOptionalFilter(rule.sources, source.source)) {
      continue;
    }

    for (const check of source.versions.checks) {
      if (!matchesOptionalFilter(rule.check_names, check.name)) {
        continue;
      }

      for (const item of check.items) {
        if (!item.upgrade_available) {
          continue;
        }
        if (!matchesOptionalFilter(rule.entity_types, item.entity_type)) {
          continue;
        }
        if (!matchesOptionalFilter(rule.entity_ids, item.entity_id)) {
          continue;
        }

        const fingerprint = buildAlertFingerprint([
          rule.name,
          source.source,
          check.name,
          item.entity_type,
          item.entity_id,
          item.latest_version || "unknown",
        ]);

        events.push({
          rule_name: rule.name,
          rule_type: rule.type,
          severity: rule.severity,
          fingerprint,
          title: `[${rule.severity.toUpperCase()}] 发现可升级版本`,
          summary:
            `${source.source}/${item.entity_type}/${item.entity_name} 可从 ${item.current_version || "unknown"} 升级到 ${item.latest_version || "latest"}`,
          detail: [
            `规则: ${rule.name}`,
            `来源: ${source.source}`,
            `检查项: ${check.name}`,
            `实体类型: ${item.entity_type}`,
            `实体: ${item.entity_name} (${item.entity_id})`,
            `当前版本: ${item.current_version || "unknown"}`,
            `最新版本: ${item.latest_version || "unknown"}`,
            "升级建议: 检查 AnyCross 本地代理宿主机或数据通道升级窗口。",
          ].join("\n"),
          labels: {
            worker_source: source.source,
            check_name: check.name,
            entity_type: item.entity_type,
            entity_id: item.entity_id,
            entity_name: item.entity_name,
            current_version: item.current_version || "",
            latest_version: item.latest_version || "",
          },
          value: 1,
        });
      }
    }
  }

  return events;
}

function evaluateAlertRule(rule, statusPayload) {
  if (rule.type === "host_status") {
    return evaluateHostStatusRule(rule, statusPayload);
  }
  if (rule.type === "host_component_status") {
    return evaluateHostComponentStatusRule(rule, statusPayload);
  }
  if (rule.type === "proxy_group_metric") {
    return evaluateProxyGroupMetricRule(rule, statusPayload);
  }
  if (rule.type === "version_upgrade_available") {
    return evaluateVersionUpgradeRule(rule, statusPayload);
  }
  return [];
}

function arrayBufferToBase64(buffer) {
  let binary = "";
  const bytes = new Uint8Array(buffer);
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

async function createFeishuSignature(secret) {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const encoder = new TextEncoder();
  const keyData = encoder.encode(`${timestamp}\n${secret}`);
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    keyData,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signBuffer = await crypto.subtle.sign("HMAC", cryptoKey, new Uint8Array());

  return {
    timestamp,
    sign: arrayBufferToBase64(signBuffer),
  };
}

async function sendFeishuTextMessage(rule, alertState, event) {
  const payload = {
    msg_type: "text",
    content: {
      text: [
        `${event.title}`,
        `${alertState === "resolved" ? "状态: 已恢复" : "状态: 告警触发"}`,
        event.summary,
        event.detail,
      ].join("\n"),
    },
  };

  if (rule.webhook_secret) {
    Object.assign(payload, await createFeishuSignature(rule.webhook_secret));
  }

  const response = await fetch(rule.webhook_url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json; charset=utf-8",
    },
    body: JSON.stringify(payload),
  });

  const body = await response.text();
  const parsed = safeJsonParse(body);
  if (!response.ok) {
    throw new Error(`Feishu webhook request failed with status ${response.status}: ${body}`);
  }
  if (parsed && parsed.code !== undefined && parsed.code !== 0) {
    throw new Error(`Feishu webhook returned code ${parsed.code}: ${parsed.msg || body}`);
  }

  return parsed || body;
}

function getAlertStateKey(rule, fingerprint) {
  return `anycross-alert:${encodeURIComponent(rule.name)}:${fingerprint}`;
}

async function loadStoredAlertState(env, key) {
  if (!env.ALERT_STATE) {
    return null;
  }
  return env.ALERT_STATE.get(key, "json");
}

async function saveStoredAlertState(env, key, value, options = {}) {
  if (!env.ALERT_STATE) {
    return;
  }

  const putOptions = {};
  // Cloudflare KV requires expirationTtl >= 60 seconds; ignore smaller values.
  if (typeof options.expirationTtl === "number" && options.expirationTtl >= 60) {
    putOptions.expirationTtl = Math.floor(options.expirationTtl);
  }

  await env.ALERT_STATE.put(key, JSON.stringify(value), putOptions);
}

async function listStoredAlertKeys(env, rule) {
  if (!env.ALERT_STATE) {
    return [];
  }

  const prefix = `anycross-alert:${encodeURIComponent(rule.name)}:`;
  const names = [];
  let cursor;
  do {
    const page = await env.ALERT_STATE.list({ prefix, cursor });
    names.push(...page.keys.map((item) => item.name));
    cursor = page.list_complete ? undefined : page.cursor;
  } while (cursor);

  return names;
}

async function processRuleNotifications(rule, events, env) {
  const now = Date.now();
  const activeByKey = new Map(
    events.map((event) => [getAlertStateKey(rule, event.fingerprint), event]),
  );
  const delivered = [];

  for (const [key, event] of activeByKey.entries()) {
    const previous = await loadStoredAlertState(env, key);
    const cooldownMs = rule.cooldown_seconds * 1000;
    const shouldSend =
      !env.ALERT_STATE ||
      !previous ||
      previous.status !== "firing" ||
      now - (previous.last_sent_at || 0) >= cooldownMs;

    if (!shouldSend) {
      continue;
    }

    await sendFeishuTextMessage(rule, "firing", event);
    delivered.push({
      state: "firing",
      event,
    });

    await saveStoredAlertState(env, key, {
      status: "firing",
      last_sent_at: now,
      event,
    });
  }

  if (!env.ALERT_STATE || !rule.notify_resolved) {
    return delivered;
  }

  const storedKeys = await listStoredAlertKeys(env, rule);
  for (const key of storedKeys) {
    if (activeByKey.has(key)) {
      continue;
    }

    const previous = await loadStoredAlertState(env, key);
    if (!previous || previous.status !== "firing" || !previous.event) {
      continue;
    }

    const resolvedEvent = {
      ...previous.event,
      title: `${previous.event.title} (已恢复)`,
      summary: `${previous.event.summary} 已恢复`,
      detail: `${previous.event.detail}\n恢复时间: ${new Date(now).toISOString()}`,
    };

    await sendFeishuTextMessage(rule, "resolved", resolvedEvent);
    delivered.push({
      state: "resolved",
      event: resolvedEvent,
    });

    // Keep the resolved marker only long enough to dedup a flapping re-fire
    // within the cooldown window, then let KV evict it automatically.
    const resolvedTtl = Math.max(rule.cooldown_seconds * 2, 3600);
    await saveStoredAlertState(
      env,
      key,
      {
        status: "resolved",
        last_sent_at: now,
        event: previous.event,
      },
      { expirationTtl: resolvedTtl },
    );
  }

  return delivered;
}

async function collectResults(url, env) {
  const configuredTargets = loadTargets(env);
  const validationError = validateTargets(configuredTargets);
  if (validationError) {
    return {
      error: validationError,
      results: [],
      targets: configuredTargets,
    };
  }

  const targets = filterTargets(configuredTargets, url);
  if (targets.length === 0) {
    return {
      error: "No matching targets found.",
      results: [],
      targets: [],
    };
  }

  return {
    error: null,
    targets,
    results: await Promise.all(targets.map((target) => collectTarget(target))),
  };
}

async function runAlertChecks(url, env) {
  const collected = await collectResults(url, env);
  if (collected.error) {
    return {
      status: 400,
      payload: {
        error: "invalid_configuration",
        message: collected.error,
      },
    };
  }

  let rules;
  try {
    rules = loadAlertRules(env);
  } catch (error) {
    return {
      status: 400,
      payload: {
        error: "invalid_configuration",
        message: error instanceof Error ? error.message : String(error),
      },
    };
  }

  const ruleValidationError = validateAlertRules(rules);
  if (ruleValidationError) {
    return {
      status: 400,
      payload: {
        error: "invalid_configuration",
        message: ruleValidationError,
      },
    };
  }

  if (rules.length === 0) {
    return {
      status: 400,
      payload: {
        error: "invalid_configuration",
        message: "No alert rules configured. Set ANYCROSS_ALERTS_JSON.",
      },
    };
  }

  const statusPayload = buildStatusPayload(collected.results);
  const checkedAt = toIsoNow();
  const ruleResults = [];
  const notifications = [];
  const deliveryErrors = [];

  for (const rule of rules) {
    const events = evaluateAlertRule(rule, statusPayload);
    let sent = [];
    let deliveryError = null;

    try {
      sent = await processRuleNotifications(rule, events, env);
    } catch (error) {
      deliveryError = error instanceof Error ? error.message : String(error);
      deliveryErrors.push({
        rule_name: rule.name,
        message: deliveryError,
      });
    }

    ruleResults.push({
      name: rule.name,
      type: rule.type,
      severity: rule.severity,
      active_alerts: events.length,
      notifications_sent: sent.length,
      notification_error: deliveryError,
      active_events: events,
    });
    notifications.push(
      ...sent.map((item) => ({
        rule_name: rule.name,
        state: item.state,
        event: item.event,
      })),
    );
  }

  return {
    status: deliveryErrors.length > 0 ? 502 : 200,
    payload: {
      checked_at: checkedAt,
      source_count: statusPayload.sources.length,
      rules: ruleResults,
      notifications,
      delivery_errors: deliveryErrors,
      kv_dedup_enabled: Boolean(env.ALERT_STATE),
    },
  };
}

async function handleAlertCheckRequest(request, env) {
  const result = await runAlertChecks(new URL(request.url), env);
  return jsonResponse(result.payload, result.status);
}

function createWorkerMetric(name, labels, value) {
  return {
    name,
    labels,
    value,
    rawValue: String(value),
  };
}

function buildSyntheticMetrics(results) {
  const metricFamilies = new Map([
    [
      "anycross_worker_target_configured",
      {
        help: "Whether a worker target scope is configured.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_target_up",
      {
        help: "Whether a worker target scope scrape completed without upstream errors.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_target_requested_ids",
      {
        help: "Configured IDs count for a worker target scope.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_target_returned_samples",
      {
        help: "Returned metric samples count for a worker target scope.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_target_scrape_errors_total",
      {
        help: "Upstream scrape errors count for a worker target scope.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_target_scrape_duration_ms",
      {
        help: "Scrape duration in milliseconds for a worker target scope.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_discovery_effective_ids",
      {
        help: "Effective IDs count after discovery fallback.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_discovery_used",
      {
        help: "Whether discovery returned IDs and replaced static configuration.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_version_items",
      {
        help: "Version check returned items count.",
        type: "gauge",
      },
    ],
    [
      "anycross_worker_version_upgrades_available",
      {
        help: "Version check upgrade-available items count.",
        type: "gauge",
      },
    ],
  ]);

  const samples = [];
  for (const result of results) {
    for (const scope of ["hosts", "proxy_groups"]) {
      const scopeResult = result[scope];
      const labels = {
        worker_source: result.source,
        scope,
      };

      samples.push(
        createWorkerMetric(
          "anycross_worker_target_configured",
          labels,
          scopeResult.configured ? 1 : 0,
        ),
      );
      samples.push(
        createWorkerMetric(
          "anycross_worker_target_up",
          labels,
          scopeResult.configured && scopeResult.errors.length === 0 ? 1 : 0,
        ),
      );
      samples.push(
        createWorkerMetric(
          "anycross_worker_target_requested_ids",
          labels,
          scopeResult.requested_ids.length,
        ),
      );
      samples.push(
        createWorkerMetric(
          "anycross_worker_target_returned_samples",
          labels,
          scopeResult.samples.length,
        ),
      );
      samples.push(
        createWorkerMetric(
          "anycross_worker_target_scrape_errors_total",
          labels,
          scopeResult.errors.length,
        ),
      );
      samples.push(
        createWorkerMetric(
          "anycross_worker_target_scrape_duration_ms",
          labels,
          scopeResult.duration_ms,
        ),
      );

      const discoveryScope = result.discovery[scope];
      samples.push(
        createWorkerMetric(
          "anycross_worker_discovery_effective_ids",
          labels,
          discoveryScope.effective_ids.length,
        ),
      );
      samples.push(
        createWorkerMetric(
          "anycross_worker_discovery_used",
          labels,
          discoveryScope.used_discovery ? 1 : 0,
        ),
      );
    }

    samples.push(
      createWorkerMetric(
        "anycross_worker_version_items",
        { worker_source: result.source },
        result.versions.totals.item_count,
      ),
    );
    samples.push(
      createWorkerMetric(
        "anycross_worker_version_upgrades_available",
        { worker_source: result.source },
        result.versions.totals.upgrade_available_count,
      ),
    );
  }

  return {
    metricFamilies,
    samples,
  };
}

function renderMetricsPayload(results) {
  const metricFamilies = new Map();
  const samples = [];

  for (const result of results) {
    for (const scope of ["hosts", "proxy_groups"]) {
      const scopeResult = result[scope];
      mergeMetricFamilies(metricFamilies, scopeResult.metric_families);
      samples.push(...scopeResult.samples);
    }
  }

  const synthetic = buildSyntheticMetrics(results);
  mergeMetricFamilies(metricFamilies, synthetic.metricFamilies);
  samples.push(...synthetic.samples);

  const lines = [];
  for (const [metricName, metadata] of [...metricFamilies.entries()].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    if (metadata.help) {
      lines.push(`# HELP ${metricName} ${escapeHelpValue(metadata.help)}`);
    }
    if (metadata.type) {
      lines.push(`# TYPE ${metricName} ${metadata.type}`);
    }
  }

  const sortedSamples = [...samples].sort((left, right) => {
    if (left.name !== right.name) {
      return left.name.localeCompare(right.name);
    }
    const leftSource = left.labels.worker_source || "";
    const rightSource = right.labels.worker_source || "";
    return leftSource.localeCompare(rightSource);
  });

  for (const sample of sortedSamples) {
    lines.push(renderSample(sample));
  }
  lines.push("# EOF");

  return lines.join("\n");
}

function formatIndexResponse(request, env) {
  const url = new URL(request.url);
  let alertRuleCount = 0;

  try {
    alertRuleCount = loadAlertRules(env).length;
  } catch {
    alertRuleCount = 0;
  }

  return jsonResponse({
    service: "anycross-monitor-worker",
    mode: "api-discovery-and-alerting",
    routes: {
      healthz: `${url.origin}/healthz`,
      metrics: `${url.origin}/metrics`,
      status: `${url.origin}/api/status`,
      metrics_json: `${url.origin}/api/metrics?format=json`,
      discovery: `${url.origin}/api/discovery`,
      versions: `${url.origin}/api/versions`,
      alert_check: `${url.origin}/api/alerts/check`,
    },
    configuration: {
      multi_target_secret: "ANYCROSS_TARGETS_JSON",
      alert_rules_secret: "ANYCROSS_ALERTS_JSON",
      legacy_single_target_supported: true,
      has_multi_target_secret: Boolean(env.ANYCROSS_TARGETS_JSON?.trim()),
      alert_rule_count: alertRuleCount,
      alert_state_kv_enabled: Boolean(env.ALERT_STATE),
    },
    usage: {
      target_filter: "Repeat ?target=<name> to filter one or more configured sources.",
      authorization:
        "If ACCESS_TOKEN is configured, send Authorization: Bearer <token> or X-Access-Token.",
    },
  });
}

async function handleFixedMetricsRequest(request, env, format) {
  const url = new URL(request.url);
  const collected = await collectResults(url, env);

  if (collected.error) {
    return jsonResponse(
      {
        error: "invalid_configuration",
        message: collected.error,
      },
      400,
    );
  }

  if (format === "json") {
    return jsonResponse(buildStatusPayload(collected.results));
  }

  return textResponse(renderMetricsPayload(collected.results), 200, {
    "Content-Type": "application/openmetrics-text; version=1.0.0; charset=utf-8",
    "Cache-Control": "no-store",
  });
}

async function handleDiscoveryRequest(request, env) {
  const collected = await collectResults(new URL(request.url), env);
  if (collected.error) {
    return jsonResponse(
      {
        error: "invalid_configuration",
        message: collected.error,
      },
      400,
    );
  }

  return jsonResponse(buildDiscoveryPayload(collected.results));
}

async function handleVersionsRequest(request, env) {
  const collected = await collectResults(new URL(request.url), env);
  if (collected.error) {
    return jsonResponse(
      {
        error: "invalid_configuration",
        message: collected.error,
      },
      400,
    );
  }

  return jsonResponse(buildVersionsPayload(collected.results));
}

async function routeRequest(request, env) {
  const authorizationError = ensureAuthorized(request, env);
  if (authorizationError) {
    return authorizationError;
  }

  const url = new URL(request.url);

  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204 });
  }

  if (request.method !== "GET") {
    return jsonResponse(
      {
        error: "method_not_allowed",
        message: "Only GET and OPTIONS are supported.",
      },
      405,
    );
  }

  if (url.pathname === "/") {
    return formatIndexResponse(request, env);
  }

  if (url.pathname === "/healthz") {
    let targets = [];
    let configurationError = null;

    try {
      targets = loadTargets(env);
      configurationError = validateTargets(targets);
    } catch (error) {
      configurationError = error instanceof Error ? error.message : String(error);
    }

    let alertRuleCount = 0;
    let alertRulesError = null;
    try {
      alertRuleCount = loadAlertRules(env).length;
    } catch (error) {
      alertRulesError = error instanceof Error ? error.message : String(error);
    }

    const alertStateKvEnabled = Boolean(env.ALERT_STATE);
    const alertCheckEnabled = normalizeBoolean(env.ALERT_CHECK_ENABLED, true);

    const warnings = [];
    if (alertRuleCount > 0 && !alertStateKvEnabled) {
      warnings.push(
        "ALERT_STATE KV is not bound; every scheduled run will resend firing alerts and cannot send resolved notifications.",
      );
    }
    if (alertRuleCount === 0 && !alertRulesError) {
      warnings.push("No alert rules configured (ANYCROSS_ALERTS_JSON empty).");
    }

    const ok = !configurationError && !alertRulesError;

    return jsonResponse(
      {
        ok,
        checked_at: toIsoNow(),
        protected: Boolean(env.ACCESS_TOKEN?.trim()),
        target_count: targets.length,
        has_multi_target_secret: Boolean(env.ANYCROSS_TARGETS_JSON?.trim()),
        alert_rule_count: alertRuleCount,
        alert_rules_error: alertRulesError,
        alert_state_kv_enabled: alertStateKvEnabled,
        alert_check_enabled: alertCheckEnabled,
        configuration_error: configurationError,
        warnings,
      },
      ok ? 200 : 503,
    );
  }

  if (url.pathname === "/metrics") {
    return handleFixedMetricsRequest(request, env, "text");
  }

  if (url.pathname === "/api/metrics") {
    const format = url.searchParams.get("format") === "json" ? "json" : "text";
    return handleFixedMetricsRequest(request, env, format);
  }

  if (url.pathname === "/api/status") {
    return handleFixedMetricsRequest(request, env, "json");
  }

  if (url.pathname === "/api/discovery") {
    return handleDiscoveryRequest(request, env);
  }

  if (url.pathname === "/api/versions") {
    return handleVersionsRequest(request, env);
  }

  if (url.pathname === "/api/alerts/check") {
    return handleAlertCheckRequest(request, env);
  }

  return jsonResponse(
    {
      error: "not_found",
      message: "Route not found.",
    },
    404,
  );
}

export default {
  async fetch(request, env) {
    try {
      return withCors(await routeRequest(request, env), request, env);
    } catch (error) {
      return withCors(
        jsonResponse(
          {
            error: "worker_error",
            message: error instanceof Error ? error.message : String(error),
          },
          500,
        ),
        request,
        env,
      );
    }
  },
  async scheduled(controller, env, ctx) {
    if (!normalizeBoolean(env.ALERT_CHECK_ENABLED, true)) {
      console.log(
        JSON.stringify({
          type: "scheduled_alert_check_skipped",
          cron: controller.cron,
          reason: "ALERT_CHECK_ENABLED is false",
        }),
      );
      return;
    }

    const task = (async () => {
      try {
        const result = await runAlertChecks(
          new URL("https://scheduled.internal/api/alerts/check"),
          env,
        );

        // "No alert rules configured" is not a failure for the cron — it just means
        // the operator hasn't wired up any rules yet. Log it at info level.
        const level =
          result.status === 200
            ? "info"
            : result.payload?.error === "invalid_configuration" &&
                /No alert rules configured/.test(result.payload?.message || "")
              ? "info"
              : "warn";

        console.log(
          JSON.stringify({
            type: "scheduled_alert_check",
            level,
            cron: controller.cron,
            status: result.status,
            payload: result.payload,
          }),
        );
      } catch (error) {
        console.error(
          "scheduled_alert_check_failed",
          error instanceof Error ? error.stack || error.message : String(error),
        );
        throw error;
      }
    })();

    if (ctx && typeof ctx.waitUntil === "function") {
      ctx.waitUntil(task);
    }
    await task;
  },
};
