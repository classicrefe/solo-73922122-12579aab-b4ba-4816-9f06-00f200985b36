# Corpus Contamination Checker

新闻语料近重复核查服务：在模型评测前找出训练集与测试集之间的近重复文章，
帮助语料工程师决定撤下哪些测试材料。

- **FastAPI + SQLite**，两个 API 进程共享同一个数据库文件（WAL）
- 以**原文连续 5 字符 shingle 集合的 Jaccard 相似度 ≥ 0.8** 连边，按连通分量分组
- 相似边与分组**持久化**，按语料代次（generation）发布；查询只读已发布结果，不做现场全量比对
- 仅当已发布结果**覆盖当前全部已提交语料**时才给出核查结论（200），否则返回 **202 + 待核查状态**

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

- API 进程 1: <http://localhost:8001/docs> （Swagger UI）
- API 进程 2: <http://localhost:8002/docs>
- 两个进程共享 `corpus-data` 卷里的同一个 SQLite 文件
- `seeder` 一次性容器会在两个 API 健康后自动载入演示语料（含桥接文章），随后退出

### 本地（无 Docker）

```bash
pip install -r requirements.txt
scripts/local_dev.sh start        # api1 -> :8001, api2 -> :8002，共库 /tmp/corpus-local/corpus.db
scripts/local_dev.sh stop
```

## 演示场景

演示语料（`scripts/demo.py`，确定性生成，内置自校验）：

| 文章 | 归属 | 说明 |
|---|---|---|
| `train-direct-1` / `test-direct-1` | train / test | 直接近重复对（相似度 ≈ 0.95） |
| `train-bridge-a` | train | 与桥接文章近重复（≈ 0.86） |
| `test-bridge` | test | **桥接文章**：同时桥接 train 侧与 test 侧 |
| `test-bridge-b` | test | 与桥接文章近重复（≈ 0.87），但与 `train-bridge-a` 仅 ≈ 0.74（不连边） |
| `train-clean-1` / `test-clean-1` | train / test | 干净对照 |

```bash
# 1) 载入演示语料并查看两个跨集合污染组
python3 scripts/demo.py seed --base http://localhost:8001

# 2) 桥接文章：删除 test-bridge -> 污染组拆开；补回 -> 组重新形成
python3 scripts/demo.py bridge --base http://localhost:8002

# 3) 修改中暂停比对：比对进行中（COMPARE_DELAY_MS=3000）连续提交两批，
#    期间查询得到 202；旧代次的计算结果被丢弃，最终只发布最新代次
python3 scripts/demo.py concurrent --base http://localhost:8001

# 4) 增量同步：取快照+游标，翻页收取修订/删除批次，写入期间继续翻页不重放
python3 scripts/demo.py sync --base http://localhost:8001

# 5) 重启：提交一批后立即重启两个进程，核查在重启后自动补完，无需重新提交
python3 scripts/demo.py restart --base http://localhost:8001 \
    --restart-cmd "docker compose restart api1 api2"
# 本地模式：--restart-cmd "scripts/local_dev.sh restart"

# 或一次跑完：demo.py all --base ... --restart-cmd ...
```

## API

| 方法/路径 | 说明 |
|---|---|
| `POST /articles/batch` | 整批补录/更正/删除：`{"upserts": [{id, side, text}], "deletes": [id]}`。**单事务原子生效**，任一失败整批回滚；成功时返回该批次的 `change_seq` |
| `GET /articles` | 当前已提交语料列表 |
| `GET /sync/snapshot` | **首次读取**：同一已提交视图返回全部文章 + 接续游标 `cursor` |
| `GET /sync/changes?cursor=…&limit=…` | **增量翻页**：按提交顺序返回游标之后的修改批次，每页最多 10 批 |
| `GET /contamination` | 核查结论。`200`：跨集合污染组、成员、直接相似边；`202`：待核查（附 current/published 代次） |
| `GET /status` | 实例、代次、语料计数、后台作业状态 |
| `GET /docs` | Swagger UI |

## 增量同步 API

离线训练语料仓的对接流程：

```text
GET /sync/snapshot                      # 全量文章 + 游标（同一已提交视图）
GET /sync/changes?cursor=<cursor>       # cursor 之后的批次，最多 10 批/页
GET /sync/changes?cursor=<next_cursor>  # 继续翻页；has_more=false 时暂停轮询
```

**快照**（`GET /sync/snapshot`）在**一个 SQLite 读事务**内同时读取全部文章、
当前修改序号和游标，游标只可能对应返回文章的那个提交视图，因此不会出现
「快照配另一代次游标」。响应：

```json
{
  "cursor": "<opaque>", "next_cursor": "<opaque>",
  "change_seq": 12, "generation": 30, "published_generation": 29,
  "article_count": 7,
  "articles": [{"id": "...", "side": "test", "text": "...", "updated_generation": 30}]
}
```

**变更页**（`GET /sync/changes`）只返回**完整批次**，批次内包含
`upserts`（新归属 + 完整新原文，即当前该 ID 的最新 side/text）与 `deletes`
（被删除的文章 ID）：

```json
{
  "cursor": "<opaque>", "next_cursor": "<opaque>",
  "from_change_seq": 12, "change_seq": 15, "latest_change_seq": 20,
  "has_more": true, "batch_count": 3,
  "batches": [
    {"seq": 13, "generation": 31, "committed_at": "...",
     "upserts": [{"id": "a1", "side": "train", "text": "..."}], "deletes": []},
    {"seq": 14, "generation": 32, "committed_at": "...",
     "upserts": [], "deletes": ["old-1"]},
    {"seq": 15, "generation": 33, "committed_at": "...",
     "upserts": [{"id": "a2", "side": "test", "text": "..."}], "deletes": ["old-2"]}
  ]
}
```

- `seq` 在**同一个写事务**内分配，SQLite 串行化写者，故 `seq` 严格等于跨实例的
  提交顺序；`cursor` 只推进到**本页实际返回的最后一批**，已确认批次不会重放，
  后续批次不会漏读或错序。
- `limit` 可小于 10，但硬上限为 10（>10 返回 422）。
- `has_more=true` 表示还有更多已提交批次，可立即继续翻页；否则轮询等待新提交。
- 空批次（`upserts`/`deletes` 均为空）不改变语料、不消耗 `seq`，`change_seq`
  返回当前序号。

**游标失效**（下游需重新取快照）：

| 情况 | 响应 |
|---|---|
| 游标格式错误/被篡改 | `400` |
| 游标属于另一个数据库（`epoch` 不同） | `409 snapshot_required` |
| 游标超出当前变更日志（库被回滚/替换） | `409 snapshot_required` |
| 游标早于保留的变更历史 | `409 snapshot_required` |

`409` 响应体：`{"status": "snapshot_required", "detail": "...", "action": "GET /sync/snapshot"}`。

**旧库启用**：打开已有数据库时只增量建表（`sync_state`/`change_*`），
已存文章与代次**原样保留**；首次快照从新 `epoch`、`seq=0` 开始提供全部旧文章，
旧部署发出的游标（`epoch` 不匹配）会得到明确的 `snapshot_required`。

校验规则：文章 ID 1–128 字符；`side ∈ {train, test}`；原文 5–2000 字符；
全库最多 **300** 篇（超出整批拒绝，409）；同批内 ID 不得重复、不得同时 upsert 与 delete。

响应示例（200）：

```json
{
  "status": "ok",
  "generation": 3,
  "contaminated": true,
  "contamination_group_count": 1,
  "same_set_group_count": 0,
  "groups": [
    {
      "group_id": 1,
      "size": 3,
      "train_members": ["train-bridge-a"],
      "test_members": ["test-bridge", "test-bridge-b"],
      "members": [{"id": "...", "side": "test", "text": "..."}],
      "edges": [
        {"a": "test-bridge", "b": "train-bridge-a", "similarity": 0.858},
        {"a": "test-bridge", "b": "test-bridge-b", "similarity": 0.868}
      ]
    }
  ]
}
```

## 设计：为什么这些需求同时成立

**语料代次（generation）** 是所有一致性的锚点：

```
提交批次  ──事务──>  articles 更新 + change_* 追加 + current_generation+1 + 比对作业入队
后台worker ──租约认领作业──> 一致性快照 ──> 计算边/组 ──> 发布事务
查询      ──> 仅当 published_generation == current_generation 才返回 200
```

- **增量同步与文章修改同事务提交**：每个 `POST /articles/batch` 在同一
  `BEGIN IMMEDIATE` 内写入 `change_batches`/`change_upserts`/`change_deletes`
  与文章本身；任一失败（校验、300 上限、ID 冲突）整体回滚，变更记录不会残留。
- **提交顺序即 `seq` 顺序**：`seq` 在写锁内分配（SQLite 串行化写者），
  两实例并发提交也得到全序，下游翻页不会乱序。
- **游标只覆盖已交付批次**：`GET /sync/changes` 的游标只推进到本页返回的最后
  一批，未返回的批次仍在 `cursor` 之后，已确认批次不会重放。
- **快照与游标同视图**：`GET /sync/snapshot` 在单个读事务内读文章、序号和游标，
  不会把快照配到另一代次/另一提交点的游标。
- **游标可验证、失效明确**：游标带数据库 `epoch` 和校验和；跨库、回滚、历史
  截断一律 `409 snapshot_required`，不会静默漏读。

- **相似边/分组持久化，查询不现场比对**：worker 把结果写入 `edges` /
  `article_groups`（按代次），`GET /contamination` 只读已发布数据。
- **两进程共库、比对时可提交**：SQLite WAL + 短 `BEGIN IMMEDIATE` 写事务；
  比对计算不持有写锁。作业存于 `compute_jobs`，带租约认领——任一时刻只有一个
  进程真正计算，另一个照常服务；进程崩溃后租约到期由存活进程接管。
- **旧计算不覆盖新修改**：发布事务内复查 `current_generation` 未变才落库，
  否则丢弃该次结果（快照时发现代次已前进也会直接丢弃作业）。
- **已发布结果不混版**：结果指针 `published_generation` 与数据在同一事务内
  原子切换；读取也在单个读事务内完成，永远只看到一个完整代次。
- **不把旧组配上新原文 / 不误报无污染**：任何提交都会使
  `current_generation > published_generation`，此时查询一律 202，
  直到覆盖最新语料的核查发布。
- **删除桥接文章即拆组**：分组是按当前语料快照重算的连通分量，桥接节点
  消失后原组自然拆分（见演示 2）。
- **整批原子性**：格式校验（pydantic）→ 单事务应用 + 300 篇上限检查，
  任一失败整体回滚。
- **重启自动补完**：启动时若 `current_generation > published_generation`
  自动补建待比对作业；作业租约机制保证崩溃进程未完成的任务被接管。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DB_PATH` | `/data/corpus.db` | SQLite 文件路径（多进程共库时指向同一文件） |
| `INSTANCE_ID` | 主机名 | 实例标识（作业认领署名） |
| `WORKER_INTERVAL_MS` | `1000` | 后台 worker 轮询间隔 |
| `COMPARE_DELAY_MS` | `0`（compose 中 3000） | 人为拉长比对耗时，便于观察 202 窗口与"修改中暂停比对" |
| `EXIT_WHEN_FILE_EXISTS` | 空 | 混沌/演示开关：该文件出现时进程立即硬退出（模拟崩溃重启） |

## 验证

```bash
python3 tests/e2e_check.py     # 34 项需求检查：校验、原子性、300 上限、202 门控、
                               # 桥接拆组、并发提交、旧算丢弃、崩溃重启恢复、不混版
python3 tests/sync_check.py    # 同步 API：快照全量+游标、批次顺序、10 批/页、
                               # 并发双实例+下游翻页不重放/不漏读/不错序、原子回滚、
                               # 旧库迁移保留文章、不可接续游标明确要求重取快照
```

## 结构

```
app/
  main.py        FastAPI 端点与生命周期
  db.py          SQLite 连接/建表（WAL）+ 变更日志表与旧库迁移
  sync.py        游标编解码、快照/变更页构建、游标有效性判定
  similarity.py  5 字符 shingle、Jaccard、连通分量
  worker.py      后台比对 worker（租约认领、快照、原子发布）
  schemas.py     请求模型（5–2000 字符等校验）
scripts/
  demo.py        演示数据（含桥接文章）+ 五个场景
  local_dev.sh   无 Docker 的双进程本地运行器
tests/
  e2e_check.py   端到端需求验证（自起两个进程共库）
  sync_check.py  增量同步端到端验证（并发双实例、旧库迁移）
docker-compose.yml / Dockerfile / requirements.txt
```
