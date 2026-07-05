# Janus Server 统一协议规范

## 1. 设计目标

Janus 协议的核心设计原则：**同一逻辑消息，三种线路编码，零损互通**。

```
                        ┌─────────────────┐
                        │  Unified Envelope │
                        │  (逻辑消息模型)    │
                        └────┬─────┬──────┘
                             │     │
              ┌──────────────┼─────┼──────────────┐
              ▼              ▼     ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────────────┐
        │ WS JSON  │  │ WS Binary│  │     gRPC         │
        │ (text帧) │  │(binary帧)│  │  (protobuf)      │
        └──────────┘  └──────────┘  └──────────────────┘
```

任意传输层接收的消息，可经任意其他传输层转发：

```
Postman (WS JSON) → S1 ──[WS JSON]──▶ S2 ──[gRPC]──▶ S3
Postman (WS Binary) → S1 ──[WS JSON]──▶ S2 ──[gRPC]──▶ S3
gRPC Client → S2 ──[gRPC]──▶ S3
```

---

## 2. 统一消息信封

所有协议共享同一个逻辑消息结构：

```
JanusEnvelope {
  ── 消息分类 ──
  method:      Method       // TALK | TALK_ONE_ANSWER_MORE | TALK_MORE_ANSWER_ONE | TALK_BIDIRECTIONAL
  mode:        Mode         // REQUEST | RESPONSE | ERROR

  ── 请求字段 (mode = REQUEST 时有效) ──
  data:        string       // 请求数据，如语言索引 "0" 或逗号分隔 "0,1,2"
  meta:        string       // 客户端标识

  ── 响应字段 (mode = RESPONSE 时有效) ──
  status:      int32        // HTTP 风格状态码：200 成功，500 错误
  results:     Result[]     // 结果列表

  ── 错误字段 (mode = ERROR 时有效) ──
  error_msg:   string       // 错误描述

  ── 链路追踪 ──
  trace_id:    string       // OpenTelemetry Trace ID
  span_id:     string       // OpenTelemetry Span ID

  ── 流控制 ──
  seq:         int32        // 序列号（0-based，流式 RPC 用于消息排序）
  stream_end:  bool         // true = 发送方完成（半关闭）
}
```

### 2.1 Method 枚举

| 值 | 常量 | 对应 gRPC 方法 | 通信模型 |
|----|------|---------------|---------|
| 0 | TALK | Talk | Unary：一请求一响应 |
| 1 | TALK_ONE_ANSWER_MORE | TalkOneAnswerMore | Server Streaming：一请求多响应 |
| 2 | TALK_MORE_ANSWER_ONE | TalkMoreAnswerOne | Client Streaming：多请求一响应 |
| 3 | TALK_BIDIRECTIONAL | TalkBidirectional | Bidi Streaming：双向流 |

### 2.2 Mode 枚举

| 值 | 常量 | 说明 |
|----|------|------|
| 0 | REQUEST | 请求消息 |
| 1 | RESPONSE | 响应消息 |
| 2 | ERROR | 错误消息 |

### 2.3 Result 结构

```
Result {
  id:   int64              // 时间戳 / 唯一 ID
  type: ResultType         // OK = 0, FAIL = 1
  kv:   map<string, string>  // 键值对数据
}
```

---

## 3. 线路编码一：WebSocket JSON

### 3.1 帧格式

WebSocket **文本帧**，UTF-8 编码的 JSON 对象。

### 3.2 请求消息

```json
{
  "method": "TALK",
  "mode": "REQUEST",
  "data": "0",
  "meta": "postman",
  "trace_id": "abc123def456",
  "span_id": "7890abcdef",
  "seq": 0,
  "stream_end": true
}
```

### 3.3 响应消息

```json
{
  "method": "TALK",
  "mode": "RESPONSE",
  "status": 200,
  "results": [
    {
      "id": 1234567890,
      "type": "OK",
      "kv": {
        "id": "uuid-aaa-bbb",
        "idx": "0",
        "data": "Hello,Thank you very much",
        "meta": "postman"
      }
    }
  ],
  "trace_id": "abc123def456",
  "span_id": "7890abcdef",
  "seq": 0,
  "stream_end": true
}
```

### 3.4 流式 RPC 交互示例

#### Server Streaming (TALK_ONE_ANSWER_MORE)

客户端发送一条 `stream_end=true` 的请求，服务端返回多条响应：

```
→ {"method":"TALK_ONE_ANSWER_MORE","mode":"REQUEST","data":"0,1,2","meta":"postman","seq":0,"stream_end":true}
← {"method":"TALK_ONE_ANSWER_MORE","mode":"RESPONSE","status":200,"results":[...],"seq":0,"stream_end":false}
← {"method":"TALK_ONE_ANSWER_MORE","mode":"RESPONSE","status":200,"results":[...],"seq":1,"stream_end":false}
← {"method":"TALK_ONE_ANSWER_MORE","mode":"RESPONSE","status":200,"results":[...],"seq":2,"stream_end":true}
```

#### Client Streaming (TALK_MORE_ANSWER_ONE)

客户端发送多条请求，最后一条 `stream_end=true`，服务端返回一条汇总响应：

```
→ {"method":"TALK_MORE_ANSWER_ONE","mode":"REQUEST","data":"0","meta":"postman","seq":0,"stream_end":false}
→ {"method":"TALK_MORE_ANSWER_ONE","mode":"REQUEST","data":"1","meta":"postman","seq":1,"stream_end":false}
→ {"method":"TALK_MORE_ANSWER_ONE","mode":"REQUEST","data":"2","meta":"postman","seq":2,"stream_end":true}
← {"method":"TALK_MORE_ANSWER_ONE","mode":"RESPONSE","status":200,"results":[...all results...],"seq":0,"stream_end":true}
```

#### Bidirectional Streaming (TALK_BIDIRECTIONAL)

请求和响应交替进行：

```
→ {"method":"TALK_BIDIRECTIONAL","mode":"REQUEST","data":"0","meta":"postman","seq":0,"stream_end":false}
← {"method":"TALK_BIDIRECTIONAL","mode":"RESPONSE","status":200,"results":[...],"seq":0,"stream_end":false}
→ {"method":"TALK_BIDIRECTIONAL","mode":"REQUEST","data":"1","meta":"postman","seq":1,"stream_end":true}
← {"method":"TALK_BIDIRECTIONAL","mode":"RESPONSE","status":200,"results":[...],"seq":1,"stream_end":true}
```

### 3.5 错误消息

```json
{
  "method": "TALK",
  "mode": "ERROR",
  "error_msg": "downstream unavailable",
  "trace_id": "abc123def456",
  "seq": 0,
  "stream_end": true
}
```

---

## 4. 线路编码二：WebSocket Binary

### 4.1 帧信封

每个 WebSocket **二进制帧**由 8 字节头 + 载荷组成：

```
偏移  长度  字段          说明
────  ────  ────────────  ──────────────────────────────────────
0     1     MAGIC         常量 0x48 ('H')
1     1     VERSION       常量 0x01
2     1     MSG_TYPE      消息类型（见 4.2）
3     1     FLAGS         保留，必须为 0x00
4     4     PAYLOAD_LEN   uint32 大端序
8     N     PAYLOAD       按消息类型编码
```

### 4.2 消息类型注册表

| 代码 | 常量 | 名称 | 方向 | 说明 |
|------|------|------|------|------|
| 0x01 | MSG_HELLO | Hello | C→S | 握手：客户端语言 |
| 0x02 | MSG_BONJOUR | Bonjour | S→C | 握手：服务端语言 |
| 0x10 | MSG_JANUS | Janus | 双向 | **统一消息**（承载全部 4 种 RPC） |
| 0x07 | MSG_PING | Ping | S→C | 心跳 |
| 0x08 | MSG_PONG | Pong | C→S | 心跳响应 |
| 0x0C | MSG_DISCONNECT | Disconnect | C→S | 断开连接 |
| 0x7F | MSG_ERROR | Error | 双向 | 协议级错误 |

### 4.3 MSG_JANUS (0x10) 载荷编码

```
偏移    类型        字段          说明
──────  ──────────  ────────────  ──────────────────────────
0       u8          method        0=TALK, 1=TALK_ONE_ANSWER_MORE,
                                    2=TALK_MORE_ANSWER_ONE, 3=TALK_BIDIRECTIONAL
1       u8          mode          0=REQUEST, 1=RESPONSE, 2=ERROR
2       i32         seq           序列号
6       u8          stream_end    0=false, 1=true
7       i32         status        状态码（RESPONSE 模式）
11      string      data          请求数据（REQUEST 模式）
        string      meta          客户端标识（REQUEST 模式）
        string      trace_id      Trace ID
        string      span_id       Span ID
        string      error_msg     错误描述（ERROR 模式）
        u32         results_count 结果数量（RESPONSE 模式）
        [Result]    results       结果数组（RESPONSE 模式）
```

**Result 编码：**

```
i64     id          时间戳 / 唯一 ID
u8      type        0=OK, 1=FAIL
kv      kv          键值对（u32 count + 每条目 string key + string value）
```

### 4.4 MSG_JANUS 编码示例

TALK 请求，data="0", meta="postman"：

```
帧头:  48 01 10 00  <PAYLOAD_LEN>
载荷:  00              method = TALK
       00              mode = REQUEST
       00 00 00 00     seq = 0
       01              stream_end = true
       00 00 00 C8     status = 0 (REQUEST 模式不使用，填 0)
       00 00 00 01 30  data = "0" (string: len=1, "0")
       00 00 00 07 70 6F 73 74 6D 61 6E  meta = "postman"
       00 00 00 00     trace_id = "" (空)
       00 00 00 00     span_id = "" (空)
```

### 4.5 基本类型编码

| 类型 | 编码 |
|------|------|
| u8 | 1 字节，无符号 |
| u32 | 4 字节，大端序，无符号 |
| i32 | 4 字节，大端序，有符号 |
| i64 | 8 字节，大端序，有符号 |
| string | u32 长度 + UTF-8 字节 |
| kv | u32 条目数 + 每条目 string key + string value |

### 4.6 错误码

| 代码 | 常量 | 说明 |
|------|------|------|
| 0x01 | ERR_DECODE | 消息解码失败 |
| 0x02 | ERR_UNKNOWN_MSG_TYPE | 未知消息类型 |
| 0x03 | ERR_TRUNCATED_PAYLOAD | 载荷长度不匹配 |
| 0x04 | ERR_BAD_MAGIC | MAGIC 不匹配 |
| 0x05 | ERR_BAD_VERSION | VERSION 不匹配 |

---

## 5. 线路编码三：gRPC (Protocol Buffers)

### 5.1 Proto 定义

```protobuf
syntax = "proto3";
package janus;

option java_multiple_files = true;
option java_package = "org.janus.proto";
option java_outer_classname = "Janus";

service JanusService {
  rpc Talk (TalkRequest) returns (TalkResponse) {}
  rpc TalkOneAnswerMore (TalkRequest) returns (stream TalkResponse) {}
  rpc TalkMoreAnswerOne (stream TalkRequest) returns (TalkResponse) {}
  rpc TalkBidirectional (stream TalkRequest) returns (stream TalkResponse) {}
}

message TalkRequest {
  string data = 1;
  string meta = 2;
  string trace_id = 3;
  string span_id = 4;
  int32 seq = 5;
  bool stream_end = 6;
}

message TalkResponse {
  int32 status = 1;
  repeated TalkResult results = 2;
  int32 seq = 3;
  bool stream_end = 4;
}

message TalkResult {
  int64 id = 1;
  ResultType type = 2;
  map<string, string> kv = 3;
}

enum ResultType {
  OK = 0;
  FAIL = 1;
}
```

### 5.2 gRPC 方法 ↔ Method 枚举映射

| gRPC 方法 | Method 值 | 通信模型 |
|-----------|-----------|---------|
| Talk | TALK (0) | Unary |
| TalkOneAnswerMore | TALK_ONE_ANSWER_MORE (1) | Server Streaming |
| TalkMoreAnswerOne | TALK_MORE_ANSWER_ONE (2) | Client Streaming |
| TalkBidirectional | TALK_BIDIRECTIONAL (3) | Bidi Streaming |

### 5.3 gRPC Metadata（追踪头）

gRPC 请求通过 Metadata 传播链路追踪上下文：

| Key | 说明 |
|-----|------|
| `x-request-id` | 请求 ID |
| `x-trace-id` | Trace ID |
| `x-span-id` | Span ID |
| `x-b3-traceid` | B3 Trace ID（兼容） |
| `x-b3-spanid` | B3 Span ID（兼容） |
| `x-b3-sampled` | B3 采样标志 |

---

## 6. 协议转换矩阵

### 6.1 转换总览

```
                     ┌──────────────────────────┐
                     │     ChainHandler          │
                     │   (协议转换层)             │
                     │                          │
  WS JSON ──decode──▶│  JanusEnvelope        │──encode──▶ WS JSON
  WS Binary ─decode─▶│  (统一逻辑消息)           │──encode──▶ WS Binary
  gRPC ──decode─────▶│                          │──encode──▶ gRPC
                     └──────────────────────────┘
```

### 6.2 字段映射表

| 统一字段 | WS JSON | WS Binary | gRPC TalkRequest | gRPC TalkResponse |
|---------|---------|-----------|------------------|-------------------|
| method | `"method":"TALK"` | payload[0] (u8) | 隐含于 RPC 方法名 | 隐含于 RPC 方法名 |
| mode | `"mode":"REQUEST"` | payload[1] (u8) | 请求 = REQUEST | 响应 = RESPONSE |
| data | `"data":"0"` | string 字段 | `data` | — |
| meta | `"meta":"postman"` | string 字段 | `meta` | — |
| status | `"status":200` | i32 字段 | — | `status` |
| results | `"results":[...]` | Result[] 字段 | — | `results` |
| trace_id | `"trace_id":"xxx"` | string 字段 | `trace_id` | Metadata: `x-trace-id` |
| span_id | `"span_id":"yyy"` | string 字段 | `span_id` | Metadata: `x-span-id` |
| seq | `"seq":0` | i32 字段 | `seq` | `seq` |
| stream_end | `"stream_end":true` | u8 字段 | `stream_end` | `stream_end` |
| error_msg | `"error_msg":"..."` | string 字段 | gRPC Status | gRPC Status |

### 6.3 转换规则

#### WS JSON → gRPC

```
1. 解析 JSON → JanusEnvelope
2. 根据 method 选择 gRPC 方法：
   TALK                  → blockingStub.talk(request)
   TALK_ONE_ANSWER_MORE  → blockingStub.talkOneAnswerMore(request) → 遍历响应
   TALK_MORE_ANSWER_ONE  → asyncStub.talkMoreAnswerOne(responseObserver)
                           → 逐条发送，最后一条 stream_end=true 时半关闭
   TALK_BIDIRECTIONAL    → asyncStub.talkBidirectional(responseObserver)
                           → 逐条发送，stream_end=true 时半关闭
3. 将 trace_id/span_id 填入 TalkRequest
4. gRPC 响应 → JanusEnvelope (mode=RESPONSE)
```

#### gRPC → WS JSON

```
1. 从 gRPC Metadata 提取 x-trace-id / x-span-id
2. TalkRequest → JanusEnvelope (mode=REQUEST, method 由 gRPC 方法名确定)
3. TalkResponse → JanusEnvelope (mode=RESPONSE)
4. 序列化为 JSON 文本帧
```

#### WS JSON → WS Binary

```
1. 解析 JSON → JanusEnvelope
2. 编码为 MSG_JANUS (0x10) 二进制帧
3. method/mode/seq/stream_end/status 编码为固定字段
4. data/meta/trace_id/span_id 编码为 string
5. results 编码为 Result 数组
```

#### WS Binary → WS JSON

```
1. 解码二进制帧 → MSG_JANUS 载荷
2. 构造 JanusEnvelope
3. 序列化为 JSON 文本帧
```

### 6.4 转换场景示例

**场景：Postman (WS JSON) → S1 (WS) → S2 (gRPC) → S3 (gRPC)**

```
Postman → S1:
  WS JSON: {"method":"TALK","mode":"REQUEST","data":"0","meta":"postman",...}

S1 → S2 (WS JSON 转发，协议不变):
  WS JSON: {"method":"TALK","mode":"REQUEST","data":"0","meta":"postman",...}

S2 → S3 (WS JSON → gRPC 转换):
  gRPC Talk(TalkRequest{data:"0", meta:"postman", trace_id:"xxx", span_id:"yyy"})

S3 → S2 (gRPC 响应):
  gRPC TalkResponse{status:200, results:[...]}

S2 → S1 (gRPC → WS JSON 转换):
  WS JSON: {"method":"TALK","mode":"RESPONSE","status":200,"results":[...],...}

S1 → Postman:
  WS JSON: {"method":"TALK","mode":"RESPONSE","status":200,"results":[...],...}
```

**场景：Client (WS Binary) → S1 (WS) → S2 (gRPC) → S3**

```
Client → S1:
  WS Binary: MSG_JANUS(method=TALK, mode=REQUEST, data="0", meta="binary-client")

S1 → S2 (WS Binary → WS JSON 转换):
  WS JSON: {"method":"TALK","mode":"REQUEST","data":"0","meta":"binary-client",...}

S2 → S3 (WS JSON → gRPC 转换):
  gRPC Talk(TalkRequest{data:"0", meta:"binary-client"})
```

---

## 7. 会话生命周期

### 7.1 WebSocket JSON 模式

```
客户端                               服务端
  │                                    │
  ├── WS Connect ─────────────────────▶│  onOpen: 创建 Session
  │                                    │
  ├── JSON Request ───────────────────▶│  onMessage: 解析 JanusEnvelope
  │                                    │  → ChainHandler 转发或本地处理
  │◀────────────── JSON Response ──────┤  返回 JanusEnvelope (RESPONSE)
  │                                    │
  ├── (重复请求/响应) ──────────────────▶│  支持流式多条消息
  │                                    │
  ├── WS Close ───────────────────────▶│  onClose: 清理 Session
```

### 7.2 WebSocket Binary 模式

```
客户端                               服务端
  │                                    │
  ├── WS Connect ─────────────────────▶│  onOpen: 创建 Session, 发送 BONJOUR
  │◀──────────────── BONJOUR ──────────┤
  │                                    │
  ├── HELLO ──────────────────────────▶│  记录客户端语言
  │                                    │
  ├── MSG_JANUS(REQUEST) ──────────▶│  解码为 JanusEnvelope
  │                                    │  → ChainHandler 转发或本地处理
  │◀──── MSG_JANUS(RESPONSE) ───────┤  返回编码后的 JanusEnvelope
  │                                    │
  │◀──────────── PING ─────────────────┤  心跳
  │─────────── PONG ──────────────────▶│  心跳响应
  │                                    │
  ├── DISCONNECT ─────────────────────▶│  关闭连接
```

### 7.3 gRPC 模式

gRPC 使用标准 HTTP/2 连接，生命周期由 gRPC 框架管理。支持：
- Health Check (`grpc.health.v1.Health`)
- Reflection (`grpc.reflection.v1.ServerReflection`)
- 4 种 RPC 模型的标准流控

---

## 8. 设计要点

### 8.1 Method 字段统一 4 种 RPC

WS 协议通过 `method` 字段显式指定 RPC 模型，使 WS 消息能精确映射到 gRPC 方法。gRPC 中 method 隐含于 RPC 方法名，无需额外字段。

### 8.2 流控制 (seq + stream_end)

| 字段 | 用途 |
|------|------|
| `seq` | 消息序列号，用于流式 RPC 中消息排序和关联 |
| `stream_end` | 标记发送方完成（半关闭），等价于 gRPC 的 `onCompleted()` |

- Unary：请求 `seq=0, stream_end=true`，响应 `seq=0, stream_end=true`
- Server Streaming：请求 `seq=0, stream_end=true`，响应 `seq=0..N, stream_end=true`
- Client Streaming：请求 `seq=0..N, stream_end=true`，响应 `seq=0, stream_end=true`
- Bidi Streaming：请求 `seq=0..N, stream_end=true`，响应 `seq=0..N, stream_end=true`

### 8.3 Trace 上下文传播

三种编码统一使用 `trace_id` + `span_id` 字段：
- WS JSON：JSON 字段 `trace_id` / `span_id`
- WS Binary：payload 中的 string 字段
- gRPC：TalkRequest 字段 `trace_id` / `span_id`（请求方向），Metadata `x-trace-id` / `x-span-id`（双向）

ChainHandler 在转发时自动注入当前 Span 的 trace_id/span_id。

### 8.4 协议扩展性

- Binary 帧 FLAGS 字节（偏移 3）保留，未来可用于压缩标志等
- MSG_TYPE 注册表可扩展新消息类型
- JanusEnvelope 可增加新字段而不破坏向后兼容（JSON 忽略未知字段，protobuf 使用 field number）

---

## 9. 实用请求构造示例

### 9.1 WebSocket JSON（通过 websocat）

```bash
# Unary TALK
echo '{"method":"TALK","mode":"REQUEST","data":"0","meta":"test","seq":0,"stream_end":true}' \
  | websocat ws://localhost:8080/json

# Server Streaming
echo '{"method":"TALK_ONE_ANSWER_MORE","mode":"REQUEST","data":"0,1,2","meta":"test","seq":0,"stream_end":true}' \
  | websocat ws://localhost:8080/json
```

### 9.2 WebSocket Binary（通过 wscat）

连接后先发送 HELLO，再发送 MSG_JANUS：

```
连接 ws://localhost:8080/binary
收到: 48 01 02 00 ...     (BONJOUR 握手)
发送: 48 01 01 00 00000001 00000000  (HELLO, lang=0)
发送: 48 01 10 00 <len> 00 00 00000000 01 00000000 00000001 30 00000004 74657374 00000000 00000000
                                    ↑   ↑  ↑          ↑         ↑           ↑
                                 TALK REQ seq=0  end  status  data="0"   meta="test"  trace=""  span=""
```

### 9.3 gRPC（通过 grpcurl）

```bash
# Unary
grpcurl -plaintext -d '{"data":"0","meta":"test"}' \
  localhost:9091 janus.JanusService/Talk

# Server Streaming
grpcurl -plaintext -d '{"data":"0,1","meta":"test"}' \
  localhost:9091 janus.JanusService/TalkOneAnswerMore

# Client Streaming（交互式）
grpcurl -plaintext -d @ localhost:9091 janus.JanusService/TalkMoreAnswerOne
# 输入:
{"data":"0","meta":"test","seq":0}
{"data":"1","meta":"test","seq":1,"stream_end":true}
# Ctrl+D 结束

# Bidirectional Streaming（交互式）
grpcurl -plaintext -d @ localhost:9091 janus.JanusService/TalkBidirectional
# 输入:
{"data":"0","meta":"test","seq":0}
{"data":"1","meta":"test","seq":1,"stream_end":true}
```

### 9.4 可观测性 API

```bash
# Prometheus 指标
curl http://localhost:9101/metrics

# Jaeger 查询 Trace
curl 'http://localhost:16686/api/traces?service=janus-server-I&limit=5'

# Loki 查询日志
curl -G 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={container_name="janus-server-I"}' --data-urlencode 'limit=10'

# Grafana API
curl -u admin:admin http://localhost:3000/api/datasources
```
