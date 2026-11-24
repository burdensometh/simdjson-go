# eventloader — 双版本事件入仓

两个上游版本的同类事件把**业务时间**和**金额**存在不同 JSON 路径上，入仓必须读到同一套
标准字段定义。本工具在结构化提取**之前**先按明确的源版本选择路径映射；同一行同时带新旧
字段时执行约定的冲突策略；回填结果逐字段标明实际使用的路径，版本无法识别的行进待补清单。

## 1. 源版本与路径映射

版本由**顶层 `schema_version` 字段**显式标识，接受 `"v1"/"v2"/"1"/"2"`（大小写、空白
容错，数字标签同样支持）。映射表在 `internal/etl/etl.go` 的 `DefaultMappings()` 中显式声明：

| 标准字段 | v1（旧版嵌套信封） | v2（新版扁平信封） |
| --- | --- | --- |
| `event_id` | `$.event.event_id` | `$.event_id` |
| `business_time` | `$.event.time`（RFC3339 字符串） | `$.biz_time`（RFC3339 字符串） |
| `amount` | `$.payment.amount`（数字或字符串数字） | `$.amount`（数字或字符串数字） |

版本未知（字段缺失、无法识别的标签如 `v3`、或非标量类型）时**不做任何猜测**，整行进待补清单。

## 2. 冲突策略：new-version-wins（按版本映射路径取值）

当同一行在新旧两条路径上都带了同一字段时：

1. **以该行声明版本所映射路径上的值为标准值**（v1 行取旧路径，v2 行取新路径）；
2. 另一条路径作为 shadow 值完整记录在 `conflicts` 中（字段、选中路径/值、影子路径/值、
   解决方式），但**绝不覆盖**标准值；
3. 两路径值相等不算冲突；映射路径缺失或值非法（如时间不是 RFC3339、金额不可解析为数字、
   值为 null）时整行进待补清单，不静默回退到另一条路径。

## 3. 输出

### 3.1 回填结果 `warehouse.ndjson`（每行一条标准记录）

```json
{
  "line_no": 3,
  "event_id": "E-1003",
  "business_time": "2026-11-21T09:00:00Z",
  "amount": 300,
  "amount_raw": "300",
  "source_version": "v1",
  "conflict_policy": "new-version-wins",
  "used_paths": {
    "event_id": "$.event.event_id",
    "business_time": "$.event.time",
    "amount": "$.payment.amount"
  },
  "conflicts": [
    {
      "field": "amount",
      "chosen_path": "$.payment.amount",
      "chosen_value": "300",
      "shadow_path": "$.amount",
      "shadow_value": "315.75",
      "resolution": "new-version-wins"
    }
  ]
}
```

- `used_paths`：每个标准字段**实际读取的路径**，满足回填结果指明路径的要求；
- `amount_raw`：金额原始词法形式，便于审计；
- `line_no`：物理行号（空行也占号）。

### 3.2 待补清单 `pending.ndjson`

```json
{
  "line_no": 6,
  "detected_version": "v3",
  "reason": "version_unknown",
  "detail": "schema_version \"v3\" does not match a known version",
  "raw_line": "{...原始整行...}"
}
```

`reason` 取值：`version_unknown`（版本未知）、`field_missing`（映射路径缺字段）、
`field_invalid`（字段类型/格式非法）。每行保留原始 JSON 供人工补录。

### 3.3 汇总（stdout）

总行数、回填数、待补数、冲突数、按版本计数、按待补原因计数。

## 4. 用法

```bash
go run ./cmd/eventloader -in samples/events.ndjson \
  -out /tmp/eventloader-out/warehouse.ndjson \
  -pending /tmp/eventloader-out/pending.ndjson
```

样例 `samples/events.ndjson` 覆盖：纯 v1、纯 v2、v1 行带新字段冲突、v2 行带旧字段冲突、
缺版本标签、未知版本 `v3`、非法时间、缺失映射字段、字符串金额、数字版本标签。

## 5. 测试

```bash
go test ./cmd/... -count=1
```

测试基于仓库自带的 `simdjson-go`（`ParseND` + `ForEach` + `FindElement` 路径查找），
覆盖版本归一化、两套路径映射、双向冲突与等值不冲突、版本未知/缺字段/非法字段进待补清单、
字符串金额、空行行号与端到端计数。
