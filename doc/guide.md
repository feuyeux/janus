# Janus Server 使用指南

## 1. 快速开始

### 1.1 前置条件

- Docker 24+
- Docker Compose v2+
- Maven 3.9+（本地构建时需要）
- JDK 25+（本地构建时需要）
- Postman 或任意 WebSocket 客户端

### 1.2 Docker Compose 一键启动

```bash
cd janus-server-java
docker compose -f docker/docker-compose.yml --project-directory . up --build
```

首次启动会自动构建 Docker 镜像并启动全部 10 个服务。等待所有服务健康后即可使用。

> 注意：`--project-directory .` 是必需的，确保构建上下文解析到项目根目录而非 `docker/` 目录。

### 1.3 验证服务状态

```bash
# 查看所有服务状态
docker compose -f docker/docker-compose.yml --project-directory . ps

# 查看 janus-server-i 启动日志
docker compose -f docker/docker-compose.yml --project-directory . logs janus-server-i
```

启动成功后，janus-server-i 会输出：
```
║ Janus Server started successfully
║   WebSocket:  ws://janus-server-i:8080/json  (JSON mode)
║   WebSocket:  ws://janus-server-i:8080/binary (Binary mode)
║   gRPC:       janus-server-i:9090
║   Metrics:    http://janus-server-i:9100/metrics
```

---

## 2. 服务端口映射

| 服务 | 容器端口 | 宿主机端口 | 用途 |
|------|---------|-----------|------|
| janus-server-i | 8080 | **8080** | WebSocket 入口（Postman 连接） |
| janus-server-i | 9090 | 9091 | gRPC |
| janus-server-i | 9100 | 9101 | Prometheus 指标 |
| janus-server-ii | 8080 | 8081 | WebSocket |
| janus-server-ii | 9090 | 9092 | gRPC |
| janus-server-ii | 9100 | 9102 | Prometheus 指标 |
| janus-server-iii | 8080 | 8082 | WebSocket |
| janus-server-iii | 9090 | 9093 | gRPC |
| janus-server-iii | 9100 | 9103 | Prometheus 指标 |
| Nacos | 8848 | **8848** | Nacos 控制台 |
| etcd | 2379 | **2379** | etcd API |
| Jaeger | 16686 | **16686** | Jaeger UI |
| Prometheus | 9090 | **9090** | Prometheus UI |
| Loki | 3100 | **3100** | Loki API |
| Grafana | 3000 | **3000** | Grafana UI（admin/admin） |

---

## 3. 请求示例（curl / Postman）

### 3.1 WebSocket JSON 模式

#### curl（通过 websocat）

```bash
# 安装 websocat（macOS）：brew install websocat
# 安装 websocat（Linux）：cargo install websocat

# 发送 Unary TALK 请求
echo '{"method":"TALK","mode":"REQUEST","data":"0","meta":"curl-test","seq":0,"stream_end":true}' \
  | websocat ws://localhost:8080/json

# 发送 Server Streaming 请求（一请求多响应）
echo '{"method":"TALK_ONE_ANSWER_MORE","mode":"REQUEST","data":"0,1,2","meta":"curl-stream","seq":0,"stream_end":true}' \
  | websocat ws://localhost:8080/json
```

#### wscat

```bash
# 安装：npm install -g wscat

# 连接后手动发送 JSON
wscat -c ws://localhost:8080/json
# 发送:
> {"method":"TALK","mode":"REQUEST","data":"1","meta":"wscat","seq":0,"stream_end":true}
```

#### Postman

1. 新建 WebSocket Request
2. 地址：`ws://localhost:8080/json`
3. 点击 Connect
4. 发送 JSON 文本消息：

**Unary 请求：**
```json
{
  "method": "TALK",
  "mode": "REQUEST",
  "data": "0",
  "meta": "postman",
  "seq": 0,
  "stream_end": true
}
```

**Server Streaming 请求：**
```json
{
  "method": "TALK_ONE_ANSWER_MORE",
  "mode": "REQUEST",
  "data": "0,1,2",
  "meta": "postman",
  "seq": 0,
  "stream_end": true
}
```

**Client Streaming 请求（发送多条，最后一条 stream_end=true）：**
```json
{"method":"TALK_MORE_ANSWER_ONE","mode":"REQUEST","data":"0","meta":"postman","seq":0,"stream_end":false}
{"method":"TALK_MORE_ANSWER_ONE","mode":"REQUEST","data":"1","meta":"postman","seq":1,"stream_end":false}
{"method":"TALK_MORE_ANSWER_ONE","mode":"REQUEST","data":"2","meta":"postman","seq":2,"stream_end":true}
```

**Bidirectional Streaming 请求：**
```json
{"method":"TALK_BIDIRECTIONAL","mode":"REQUEST","data":"0","meta":"postman","seq":0,"stream_end":false}
{"method":"TALK_BIDIRECTIONAL","mode":"REQUEST","data":"1","meta":"postman","seq":1,"stream_end":true}
```

> **⚠️ WS 入口的流式限制**：经 WebSocket 入口发送时，客户端流（`TALK_MORE_ANSWER_ONE`）与双向流（`TALK_BIDIRECTIONAL`）会被桥接层**收敛为一元调用**（每条 WS 消息各自触发一次独立的一元请求-响应）；服务端流（`TALK_ONE_ANSWER_MORE`）会聚合为单个响应帧返回。要体验**真正的**四种流式语义，请用 `grpcurl` 直连原生 gRPC 入口（见 [§3.4](#34-grpc-测试grpcurl)），机制详见 [协议规范 §6.3](protocol.md#63-转换规则)。

**响应消息（Unary 示例）：**
```json
{
  "status": 200,
  "results": [
    {
      "id": 123456789,
      "type": "OK",
      "kv": {
        "id": "uuid-xxx",
        "idx": "0",
        "data": "Hello,Thank you very much",
        "meta": "postman"
      }
    }
  ]
}
```

请求会经过完整链路：S1 (WS) → S2 (WS) → S3 (gRPC) → 本地处理 → 响应原路返回。

### 3.2 WebSocket Binary 模式

1. 新建 WebSocket Request
2. 输入地址：`ws://localhost:8080/binary`
3. 点击 Connect
4. 收到 BONJOUR 消息后，发送二进制 ECHO_REQUEST

ECHO_REQUEST 帧结构（8 字节头 + 载荷）：

```
48 01 03 00  <payload_len>  <echo_id i64>  <meta string>  <data string>  <trace_id string>  <span_id string>
```

详见 [协议文档](protocol.md#2-websocket-binary-协议)。

### 3.3 多语言索引测试

`data` 字段支持 0-5 的语言索引：

| data | 问候语 | 回应 |
|------|--------|------|
| 0 | Hello | Thank you very much |
| 1 | Bonjour | Merci beaucoup |
| 2 | Hola | Muchas Gracias |
| 3 | こんにちは | どうも ありがとう ございます |
| 4 | Ciao | Mille Grazie |
| 5 | 안녕하세요 | 대단히 감사합니다 |

### 3.4 gRPC 测试（grpcurl）

```bash
# 安装 grpcurl（macOS）：brew install grpcurl
# 安装 grpcurl（Linux）：go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# 列出服务
grpcurl -plaintext localhost:9091 list

# 列出方法
grpcurl -plaintext localhost:9091 list janus.JanusService

# Unary 调用
grpcurl -plaintext -d '{"data":"0","meta":"grpcurl-test"}' \
  localhost:9091 janus.JanusService/Talk

# Server Streaming 调用
grpcurl -plaintext -d '{"data":"0,1,2","meta":"grpcurl-stream"}' \
  localhost:9091 janus.JanusService/TalkOneAnswerMore
```

### 3.5 可观测性 curl 查询

```bash
# Prometheus 指标（从 S1）
curl http://localhost:9101/metrics

# Prometheus 查询 RPC 调用总数
curl 'http://localhost:9090/api/v1/query?query=rpc_calls_total'

# Prometheus 查看所有采集目标状态
curl http://localhost:9090/api/v1/targets | python3 -m json.tool

# Jaeger 查看所有服务
curl http://localhost:16686/api/services

# Jaeger 查看 janus-server-i 的最近 10 条 Trace
curl 'http://localhost:16686/api/traces?service=janus-server-i&limit=10'

# Loki 查看所有标签
curl http://localhost:3100/loki/api/v1/labels

# Loki 查询 janus-server-i 的日志
curl -G 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={container_name="janus-server-i"}' \
  --data-urlencode 'limit=10'

# Loki 按 traceId 查询跨节点日志
curl -G 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={traceId="<your-trace-id>"}' \
  --data-urlencode 'limit=50'

# Grafana 数据源列表
curl -u admin:admin http://localhost:3000/api/datasources

# Nacos 健康检查
curl http://localhost:8848/nacos/v1/ns/operator/metrics

# etcd 查看已注册的服务
docker exec etcd etcdctl get --prefix janus-server
```

---

## 4. 可观测性使用

### 4.1 Grafana（统一查询）

访问 `http://localhost:3000`，用户名/密码：`admin` / `admin`

#### 查看日志（Loki）

1. 左侧菜单 → Explore
2. 选择数据源：**Loki**
3. LogQL 查询示例：

```logql
# 查看所有 janus 服务日志
{service=~"janus-server.*"}

# 按服务名过滤
{service="janus-server-i"}

# 按日志级别过滤
{service=~"janus-server.*", level="ERROR"}

# 按 Trace ID 关联查询跨节点日志
{service=~"janus-server.*", traceId="abc123def456"}

# 搜索关键字
{service=~"janus-server.*"} |= "Forwarding"
```

#### 查看指标（Prometheus）

1. 选择数据源：**Prometheus**
2. PromQL 查询示例：

```promql
# gRPC 调用总数（按方法分组）
sum(rpc_calls_total) by (method)

# WebSocket 消息总数（按类型分组）
sum(ws_messages_total) by (type)

# 各服务 RPC 调用速率
rate(rpc_calls_total[1m])
```

#### 查看追踪（Jaeger）

1. 选择数据源：**Jaeger**
2. 选择 Service：`janus-server-i`
3. 点击 Find Traces
4. 展开 Trace 可看到完整链路：S1 → S2 → S3

### 4.2 Jaeger UI

访问 `http://localhost:16686`

- 直接搜索 Trace，按 Service / Operation / Tags 过滤
- 查看 Span 详情，包含 traceId / spanId
- 支持 Trace 时间线视图

### 4.3 Prometheus UI

访问 `http://localhost:9090`

- 直接执行 PromQL 查询
- 查看已采集的 Target 状态（Status → Targets）
- 查看告警规则

### 4.4 三信号关联查询

完整的可观测性关联流程：

```
1. 在 Grafana → Loki 中搜索日志，发现异常
   {service="janus-server-ii"} |= "ERROR"

2. 从日志中获取 traceId（Loki 标签或日志字段）

3. 在 Grafana → Jaeger 中按 traceId 查询完整链路
   查看哪个节点的 Span 耗时最长

4. 在 Grafana → Prometheus 中查询对应时间段的指标
   rate(rpc_calls_total[1m])

5. 或反向：从 Jaeger Trace 跳转到 Loki 日志
   Jaeger → Trace Detail → Logs → 点击跳转
```

---

## 5. 本地构建与运行

### 5.1 Maven 构建

```bash
cd janus-server-java
mvn clean package -DskipTests
```

构建产物：`target/janus.jar`（Fat JAR，约 42MB）

### 5.2 本地运行（单节点模式）

不配置下游，直接处理请求：

```bash
java -jar target/janus.jar
```

默认配置：
- WebSocket: `ws://localhost:8080/json`
- gRPC: `localhost:9090`
- Metrics: `http://localhost:9100/metrics`

### 5.3 本地多节点链路运行

需要先启动 Nacos 和 etcd（可通过 Docker 单独启动）：

```bash
# 启动 Nacos
docker run -d --name nacos -p 8848:8848 -p 9848:9848 \
  -e MODE=standalone \
  -e NACOS_AUTH_TOKEN=SmFudXNTZXJ2ZXJBdXRoVG9rZW4yMDI2U2VjcmV0S2V5 \
  -e NACOS_AUTH_IDENTITY_KEY=janus \
  -e NACOS_AUTH_IDENTITY_VALUE=janus \
  nacos/nacos-server:v3.2.2

# 启动 etcd
docker run -d --name etcd -p 2379:2379 \
  -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
  -e ALLOW_NONE_AUTHENTICATION=yes \
  quay.io/coreos/etcd:v3.6.13

# 启动 Jaeger
docker run -d --name jaeger -p 16686:16686 -p 4317:4317 \
  -e COLLECTOR_OTLP_ENABLED=true \
  jaegertracing/all-in-one:1.76.0
```

终端节点 (S3)：
```bash
java -jar target/janus.jar \
  -DJANUS_SERVER_ID=server-iii \
  -DJANUS_REGISTER=etcd \
  -DJANUS_ETCD_ENDPOINT=http://localhost:2379 \
  -DJANUS_ADVERTISED_HOST=localhost \
  -DJANUS_OTEL_ENABLED=Y \
  -DOTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  -DOTEL_SERVICE_NAME=janus
```

中间节点 (S2)：
```bash
java -jar target/janus.jar \
  -DJANUS_SERVER_ID=server-ii \
  -DJANUS_DOWNSTREAM_PROTOCOL=grpc \
  -DJANUS_DOWNSTREAM_DISCOVERY=etcd \
  -DJANUS_REGISTER=nacos \
  -DJANUS_NACOS_ENDPOINT=localhost:8848 \
  -DJANUS_ETCD_ENDPOINT=http://localhost:2379 \
  -DJANUS_ADVERTISED_HOST=localhost \
  -DJANUS_OTEL_ENABLED=Y \
  -DOTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  -DOTEL_SERVICE_NAME=janus
```

入口节点 (S1)：
```bash
java -jar target/janus.jar \
  -DJANUS_SERVER_ID=server-i \
  -DJANUS_DOWNSTREAM_PROTOCOL=ws \
  -DJANUS_DOWNSTREAM_DISCOVERY=nacos \
  -DJANUS_NACOS_ENDPOINT=localhost:8848 \
  -DJANUS_ADVERTISED_HOST=localhost \
  -DJANUS_OTEL_ENABLED=Y \
  -DOTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  -DOTEL_SERVICE_NAME=janus
```

> 注意：`OTEL_SERVICE_NAME` 是基础名称，代码会自动追加 `-{SERVER_ID}` 后缀（如 `janus-server-i`）。

### 5.4 可选：WebSocket 共享令牌鉴权

默认情况下 WebSocket 入口不做鉴权（保持开放）。如需为入站连接加一层共享密钥校验，可设置 `JANUS_AUTH_TOKEN`：

```bash
export JANUS_AUTH_TOKEN=my-secret-token
java -jar target/janus.jar
```

行为说明：

- **留空（默认）**：不校验，行为与之前完全一致。
- **设置后**：入站 WS 握手必须携带匹配的 `authToken` 请求头，否则连接会被以 `POLICY_VALIDATION`（1008）关闭；链路上游节点的 WS 转发客户端会自动带上该头，因此**链路中的每个节点都需配置相同的 `JANUS_AUTH_TOKEN`**。

客户端示例（`wscat` 通过 `-H` 传头）：

```bash
wscat -H "authToken: my-secret-token" -c ws://localhost:8080/json
```

> 提示：该机制仅提供最基础的共享密钥校验，不替代传输层加密。若需端到端加密，请在反向代理层启用 `wss://` / TLS。

---

## 6. 停止与清理

### 停止所有服务

```bash
docker compose -f docker/docker-compose.yml --project-directory . down
```

### 停止并清除数据卷

```bash
docker compose -f docker/docker-compose.yml --project-directory . down -v
```

### 重新构建镜像

```bash
docker compose -f docker/docker-compose.yml --project-directory . build --no-cache
```

---

## 7. 故障排查

### 7.1 服务启动失败

```bash
# 查看具体服务日志
docker compose -f docker/docker-compose.yml logs janus-server-i

# 常见问题：
# - Nacos 未就绪 → 等待 healthcheck 通过（约 30 秒）
# - etcd 未就绪 → 等待 healthcheck 通过（约 10 秒）
# - 端口冲突 → 检查宿主机端口占用
```

### 7.2 链路不通

```bash
# 检查 S2 是否注册到 Nacos
# Nacos v3 已移除 v1 REST API，可通过容器日志确认注册状态：
docker logs nacos 2>&1 | grep janus-server

# 检查 S3 是否注册到 etcd
docker exec etcd etcdctl get --prefix janus-server

# 检查 S1 是否能发现 S2
docker logs janus-server-i 2>&1 | grep "Discovered"
```

### 7.3 追踪数据缺失

```bash
# 检查 Jaeger 是否收到 Trace
# 访问 http://localhost:16686 → 查看是否有 Service 列表

# 检查 OTLP 连接
docker logs janus-server-i 2>&1 | grep "OpenTelemetry"
```

### 7.4 日志未采集

```bash
# 检查 Promtail 状态
docker logs promtail

# 检查 Loki 是否就绪
curl http://localhost:3100/ready

# 在 Grafana 中查询 Loki
# {service=~".*"} 查看是否有任何日志
```
