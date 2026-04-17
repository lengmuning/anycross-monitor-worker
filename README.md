# AnyCross Monitor Worker

用于 Cloudflare Worker 的飞书 AnyCross 本地代理监控服务，支持：

- 宿主机监控
- 数据通道 / 代理集群监控
- 多目标监控
- 通过额外 API 自动发现 `hosts` / `proxy_groups`
- 通过额外 API 检查“有新版本可升级”
- 通过飞书机器人推送异常、故障、升级提醒
- Cloudflare `scheduled` 定时巡检

参考文档：

- [监控本地代理服务](https://anycross.feishu.cn/documentation/platform/monitor-on-prem-service)

已接入的官方监控接口：

- `GET /api/agent/v2/monitor/hosts/components/process/status/metrics`
- `GET /api/agent/v2/monitor/proxyGroups/runtime/metrics`

## 路由

- `GET /metrics`: 聚合后的 OpenMetrics
- `GET /api/status`: 完整 JSON 状态，包含 discovery / metrics / versions
- `GET /api/metrics?format=json`: 同 `/api/status`
- `GET /api/discovery`: 仅查看自动发现结果
- `GET /api/versions`: 仅查看版本检查结果
- `GET /api/alerts/check`: 立即执行一次告警检查并推送飞书
- `GET /healthz`: 配置与存活检查

## AnyCross 已知监控参数

宿主机监控接口要求：

- `api_key`
- 重复的 `host` 参数
- 单次最多 10 个 `host`

代理集群监控接口要求：

- `api_key`
- 重复的 `proxy_group` 参数
- 单次最多 10 个 `proxy_group`

## 配置

### 1. 推荐方式：`ANYCROSS_TARGETS_JSON`

```bash
wrangler secret put ANYCROSS_TARGETS_JSON
```

最小示例：

```json
[
  {
    "name": "tenant-a",
    "base_url": "https://anycross.feishu.cn",
    "api_key": "api-key-a",
    "hosts": ["host_1", "host_2"],
    "proxy_groups": ["pg_1"]
  }
]
```

字段说明：

- `name`: 目标名称，会写入 `worker_source` 标签
- `base_url`: 可选，默认 `https://anycross.feishu.cn`
- `api_key`: AnyCross 监控 API Key
- `hosts`: 可选，静态宿主机列表
- `proxy_groups`: 可选，静态代理集群 / 数据通道列表
- `vars`: 可选，自定义变量，可被 discovery / version 配置里的 `{{vars.xxx}}` 引用
- `discovery`: 可选，自动发现配置
- `version_checks`: 可选，版本检查配置

### 2. 自动发现配置

如果你已经拿到了 AnyCross 的列表类 API，可以让 Worker 先发现，再监控。

示例：

```json
[
  {
    "name": "tenant-a",
    "base_url": "https://anycross.feishu.cn",
    "api_key": "api-key-a",
    "discovery": [
      {
        "name": "discover-hosts",
        "scope": "hosts",
        "path": "/discover/hosts",
        "auth_mode": "api_key_query",
        "item_path": "data.items",
        "id_path": "id"
      },
      {
        "name": "discover-proxy-groups",
        "scope": "proxy_groups",
        "path": "/discover/proxy-groups",
        "auth_mode": "api_key_query",
        "item_path": "data.items",
        "id_path": "id"
      }
    ]
  }
]
```

发现逻辑：

- 如果 discovery 成功拿到 ID，则用 discovery 结果发起监控
- 如果 discovery 失败或返回空列表，则回退到静态 `hosts` / `proxy_groups`

支持字段：

- `name`
- `scope`: `hosts` 或 `proxy_groups`
- `path` 或 `url`
- `method`: 默认 `GET`
- `auth_mode`: `api_key_query` / `api_key_header` / `bearer` / `header` / `cookie` / `none`
- `api_key_param`: `auth_mode=api_key_query` 时默认 `api_key`
- `auth_header_name`
- `auth_value`
- `query`
- `headers`
- `item_path`: JSON 数组路径
- `id_path`: 每个元素里 ID 字段路径
- `name_path`: 可选，仅用于调试展示

### 3. 版本升级检查

如果你有“宿主机版本 / 数据通道版本 / 本地代理版本”接口，可以直接接进 Worker。

示例：

```json
[
  {
    "name": "tenant-a",
    "base_url": "https://anycross.feishu.cn",
    "api_key": "api-key-a",
    "version_checks": [
      {
        "name": "host-upgrades",
        "entity_type": "host",
        "path": "/versions/hosts",
        "auth_mode": "api_key_query",
        "item_path": "data.items",
        "id_path": "id",
        "name_path": "name",
        "current_version_path": "current_version",
        "latest_version_path": "latest_version"
      },
      {
        "name": "channel-upgrades",
        "entity_type": "proxy_group",
        "path": "/versions/channels",
        "auth_mode": "api_key_query",
        "item_path": "data.items",
        "id_path": "id",
        "name_path": "name",
        "current_version_path": "current",
        "latest_version_path": "latest",
        "upgrade_available_path": "upgrade_available"
      }
    ]
  }
]
```

版本判断逻辑：

- 如果配置了 `upgrade_available_path`，优先使用它
- 否则比较 `current_version_path` 和 `latest_version_path`
- 两者不同即视为“可升级”

### 4. 旧版单目标模式

仍兼容旧配置：

```bash
wrangler secret put ANYCROSS_API_KEY
```

`wrangler.toml` 可选变量：

- `ANYCROSS_BASE_URL`
- `DEFAULT_HOST_IDS`
- `DEFAULT_PROXY_GROUP_IDS`
- `DEFAULT_SOURCE_NAME`
- `ALLOWED_ORIGIN`

### 5. 访问保护

```bash
wrangler secret put ACCESS_TOKEN
```

请求时带：

```bash
curl -H 'Authorization: Bearer <ACCESS_TOKEN>' \
  'https://<your-worker>/api/status'
```

## 飞书机器人告警

```bash
wrangler secret put ANYCROSS_ALERTS_JSON
wrangler secret put FEISHU_WEBHOOK_URL
wrangler secret put FEISHU_WEBHOOK_SECRET
```

示例规则：

```json
[
  {
    "name": "host-offline",
    "type": "host_status",
    "severity": "critical",
    "statuses": ["offline", "unknown"],
    "cooldown_seconds": 900,
    "notify_resolved": true
  },
  {
    "name": "component-offline",
    "type": "host_component_status",
    "severity": "warning",
    "component_types": ["agenthub"],
    "statuses": ["offline", "unknown"]
  },
  {
    "name": "proxy-qps-high",
    "type": "proxy_group_metric",
    "severity": "warning",
    "metric": "proxy_group_runtime_http_proxy_qps",
    "aggregate": "max",
    "op": ">=",
    "threshold": 100
  },
  {
    "name": "upgrade-reminder",
    "type": "version_upgrade_available",
    "severity": "info",
    "entity_types": ["host", "proxy_group"],
    "check_names": ["host-upgrades", "channel-upgrades"],
    "cooldown_seconds": 43200,
    "notify_resolved": true
  }
]
```

支持规则类型：

- `host_status`
- `host_component_status`
- `proxy_group_metric`
- `version_upgrade_available`

公共字段：

- `name`
- `severity`
- `sources`
- `cooldown_seconds`
- `notify_resolved`
- `webhook_url`: 不填则走 `FEISHU_WEBHOOK_URL`
- `webhook_secret`: 不填则走 `FEISHU_WEBHOOK_SECRET`

规则过滤字段：

- `host_status`: `host_ids`, `statuses`
- `host_component_status`: `host_ids`, `component_ids`, `component_types`, `statuses`
- `proxy_group_metric`: `proxy_group_ids`, `metric`, `aggregate`, `op`, `threshold`, `group_by`, `label_filters`
- `version_upgrade_available`: `entity_ids`, `entity_types`, `check_names`

## 定时巡检

`wrangler.toml` 已内置：

```toml
[triggers]
crons = ["*/5 * * * *"]
```

Cloudflare cron 使用 UTC 时间。调整为 `*/1 * * * *` 可获得更低延迟（代价是更多调用次数）。

可用 `ALERT_CHECK_ENABLED = "false"` 在不删除 cron 的情况下临时禁用定时巡检。

## KV 去重

**强烈建议绑定 KV**，否则每轮 `scheduled` 都会把所有仍在告警的事件重复推给飞书。

```bash
wrangler kv:namespace create ALERT_STATE
```

把生成的 id 填入 `wrangler.toml`（绑定名固定为 `ALERT_STATE`）：

```toml
[[kv_namespaces]]
binding = "ALERT_STATE"
id = "<kv-namespace-id>"
```

有 KV 时：

- 同一告警在冷却时间内不重复推送
- 状态恢复时会推送 “已恢复”
- 已恢复记录会在 `max(cooldown * 2, 1h)` 后自动过期，避免 KV 堆积

无 KV 时：

- 每次巡检命中都会推送（会被飞书机器人刷屏）
- 没有 “已恢复” 通知
- `/healthz` 会在 `warnings` 字段里明确提示

## 本地开发

```bash
npm install
npm run dev
```

## 部署

```bash
npm run deploy
```

## 常用调用

```bash
curl 'https://<your-worker>/metrics'
curl 'https://<your-worker>/api/status'
curl 'https://<your-worker>/api/discovery'
curl 'https://<your-worker>/api/versions'
curl 'https://<your-worker>/api/alerts/check'
```

按目标过滤：

```bash
curl 'https://<your-worker>/api/status?target=tenant-a'
curl 'https://<your-worker>/api/alerts/check?target=tenant-a'
```

## Worker 额外指标

除了上游 OpenMetrics，Worker 还会导出：

- `anycross_worker_target_configured`
- `anycross_worker_target_up`
- `anycross_worker_target_requested_ids`
- `anycross_worker_target_returned_samples`
- `anycross_worker_target_scrape_errors_total`
- `anycross_worker_target_scrape_duration_ms`
- `anycross_worker_discovery_effective_ids`
- `anycross_worker_discovery_used`
- `anycross_worker_version_items`
- `anycross_worker_version_upgrades_available`

## 说明

当前代码已经支持“自动发现 + 故障告警 + 升级提醒”的执行链路，但 AnyCross 哪些列表接口、版本接口可直接公开调用，仍取决于你实际掌握的 API。

我目前确认到的前端接口线索里，至少存在一个与本地代理组可见范围相关的接口：

- `GET /api/agenthub/ahweb/hubgroup/listViewable`

如果你后面把真实的“宿主机列表接口”和“版本接口”路径、字段结构给我，我可以继续帮你把 `ANYCROSS_TARGETS_JSON` 直接改成你的生产可用配置。
