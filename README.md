# 边缘网关值班切换 Demo

FastAPI + SQLite 实现的边缘网关与操作网页。值班员切换处理较久的 HTTP 服务上游时，
已接纳的请求仍从原上游取回完整响应；新请求在确认切换后立即走新目标。

## 组成

| 组件 | 说明 |
| --- | --- |
| `gateway/` | FastAPI 网关：转发请求、操作 API、值班台网页（`/`）、SQLite 持久化 |
| `upstream/` | 演示上游：可设默认响应延迟（`DEFAULT_DELAY_MS`），请求参数 `delay_ms` 可覆盖；`/health` 供切换前探测 |
| `docker-compose.yml` | 一键启动网关 + 两个本地上游（alpha 默认 3s、beta 默认 0.5s） |
| `scripts/` | 本地（无 Docker）端到端验证脚本 |

## 运行

Docker Compose：

```bash
docker compose up --build
# 值班台网页: http://localhost:8080/
```

本地直接跑（无需 Docker，含自动化断言与重启演练）：

```bash
./scripts/run_local.sh
```

## 语义与实现要点

- **已接纳请求走原上游**：`/api/call` 在接纳时原子地快照当前路由并登记在途
  （`db.admit`），随后无论路由如何切换，都从快照目标取回完整响应。
- **先探测后确认**：`POST /api/switch/probe` 只探测候选（`/health` + `/work`），
  从不改写路由——探测期间请求继续走旧目标，探测失败维持原路由；
  `POST /api/switch/commit` 才把路由切到新目标。
- **立即生效**：确认切换只做一次版本守卫的单行 `UPDATE`，不等待旧请求结束；
  之后接纳的请求立即走新目标。
- **可下线提示**：非当前目标的在途数清零时，状态接口给出 `can_offline: true`，
  网页对应行显示“可下线 ✓”。
- **版号防交错**：网页每次操作都携带它所见的路由版号；确认切换按
  `version = base_version` 做比较并交换（CAS）。两次切换交错时，先到的确认生效，
  迟到的确认（含迟到探测发起的确认）收到 `409 version_conflict`，不得覆盖已生效路由。
- **重启恢复**：路由与上游登记在 SQLite 中持久化，重启后按已登记路由接流量；
  启动时清空 `inflight` 表，被中断的请求不会留作在途（清理条数在网页顶部展示）。

## 操作 API

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/state` | 当前路由（目标+版号）、各上游在途数、在途明细、最近探测 |
| `POST /api/call` | 发起转发请求 `{delay_ms?, path?}`，返回服务目标与上游响应 |
| `POST /api/switch/probe` | 探测候选 `{target, base_version}`，不改路由 |
| `POST /api/switch/commit` | 确认切换 `{target, base_version, probe_id}`，版号冲突返回 409 |

## 验证

`scripts/run_local.sh` 启动两个上游与网关，执行 `scripts/smoke_test.py`
两阶段共 32 项断言：切换语义、探测失败维持路由、交错切换版号冲突、
慢请求从原上游取回完整响应、在途清零才可下线、`kill -9` 后重启的在途清理与路由保持。
