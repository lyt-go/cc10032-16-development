# 流批一体数据集成平台

面向实时流接入、批流合并、变更捕获与分区治理的流批一体数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式数据源 | `stream_source` | 流式源配置 | 源标识、消息中间件、Topic名称 |
| 批次分片 | `batch_slice` | 分片批次 | 批次编号、源表名称、分片键列 |
| 变更捕获 | `change_capture` | 变更捕获配置 | 配置编号、源数据库、目标表 |
| Schema治理 | `schema_govern` | Schema版本 | 主题名称、Schema定义、版本号 |
| 水位管理 | `watermark` | 消费水位 | 分区标识、消费组、当前位点 |
| 迟到数据 | `late_data` | 迟到数据策略 | 策略编号、关联源、允许延迟秒数 |
| 流式关联 | `stream_lookup` | 关联规则 | 规则编号、左流来源、右表来源 |
| 合并策略 | `merge_plan` | 合并计划 | 计划编号、流式来源、批式来源 |
| 分区扩缩 | `partition_scale` | 分区策略 | 策略编号、目标表、分区字段 |
| 快照管理 | `snapshot_mgr` | 数据快照 | 快照编号、源表名称、快照时间 |
| 数据对账 | `data_reconcile` | 对账任务 | 任务编号、左源表、右源表 |
| 回填作业 | `backfill_job` | 回填任务 | 任务编号、目标表、回填起始时间 |
| 冷热分层 | `cold_archive` | 归档策略 | 策略编号、源表名称、热数据天数 |
| 压缩合并 | `compaction` | 压缩任务 | 任务编号、目标表、目标分区 |
| 血缘图 | `lineage` | 血缘关系 | 关系编号、上游表、下游表 |
| 变更日志 | `changelog` | 变更记录 | 记录编号、目标表、变更类型 |
| 保留管理 | `retention` | 保留策略 | 策略编号、目标表、保留天数 |
| 表格式 | `table_format` | 表格式配置 | 配置编号、目标表、表格式类型 |

## 快照交接包与回填窗口

快照管理页内置“快照交接包”面板；后端核心实现在
`backend/app/handoff.py`，HTTP 接口前缀为 `/api/snapshot-handoff`。

交接包是自描述 JSON，包含：

- 源表名称、快照编号、快照版本和业务键；
- 每个分段的段号、行数、SHA-256/MD5 内容凭证；
- 完整业务行和整包内容凭证，可在另一环境直接导入；
- 导入后同步到 `store.handoff.list_baselines()` 和
  `store._backfill_baseline_manifest`。

核心接口：

```text
POST /api/snapshot-handoff/packages/export       # 生成完整交接包
POST /api/snapshot-handoff/packages/verify       # 核对源表/版本/分段/凭证
POST /api/snapshot-handoff/imports               # 创建分段导入候选
POST /api/snapshot-handoff/imports/{id}/segments/{n}
POST /api/snapshot-handoff/imports/{id}/interrupt
POST /api/snapshot-handoff/imports/{id}/resume
POST /api/snapshot-handoff/imports/{id}/complete
POST /api/snapshot-handoff/packages/import       # 一次性完整导入
GET  /api/snapshot-handoff/baselines             # 回填基准清单
POST /api/snapshot-handoff/backfill-windows      # 从已核验基准实际回填
POST /api/snapshot-handoff/backfill-windows/compare
GET  /api/snapshot-handoff/conflicts
POST /api/snapshot-handoff/candidates/{id}/register-alternative
```

登记规则：

1. 相同编号、快照版本和内容凭证的重复导入只返回 `duplicate/idempotent`
   确认，不新增基准。
2. 相同编号和版本但内容凭证变化时，新候选进入 `quarantined` 隔离区，
   原回填基准不变；可使用新编号或新版本另行登记。
3. 同一编号已有首段核验通过的候选时，后到的不同内容完整包会得到并发冲突提示。
   默认 `registered_version_wins` 保留首个完整核验并登记者，同时在回填窗口中明示
   未完候选仍待哪一段；若要让首段候选悲观占用编号，可使用
   `SnapshotHandoffRegistry(conflict_policy="first_verified_segment_wins")`。
   冲突响应始终给出 `actual_retained_candidate`、`loser_candidate` 和败方下一步。
4. 回填窗口只接受 `complete_verified` 基准。若该编号只有未完候选，
   窗口返回 `blocked_incomplete` 和 `missing_segment_nos`，明确仍待哪一段。
5. 窗口结果包含每个业务键的期望行、实际行、状态和字段差异；两个窗口可通过
   `compare_backfill_windows` 比较。

验收测试：

```bash
cd backend
.venv/bin/pytest -q tests/test_snapshot_handoff.py
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
