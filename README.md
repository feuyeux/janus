# Janus Server

> **Janus** — the Roman god of doorways, beginnings, and transitions, depicted with two faces looking in opposite directions. One face gazes toward WebSocket, the other toward gRPC — a fitting metaphor for a server that bridges protocols, translates messages, and routes requests across multi-node service chains.

<img src="logo.png" style="width:325px" />

统一通信服务器，同时支持 **WebSocket JSON**、**WebSocket Binary** 和 **gRPC（4 种通信模型）** 协议。通过 Nacos / etcd 实现服务注册与发现，内置 OpenTelemetry 全链路可观测能力（Tracing / Metrics / Logging）。

## 特性

- **多协议统一**：WebSocket JSON、WebSocket Binary、gRPC 三种传输协议共享同一逻辑消息模型，任意协议接收的请求可透明转发至其他协议
- **4 种 gRPC 通信模型**：Unary、Server Streaming、Client Streaming、Bidirectional Streaming
- **服务注册与发现**：支持 Nacos 和 etcd，gRPC 客户端内置自定义 NameResolver 实现自动负载均衡
- **全链路可观测**：OpenTelemetry SDK 统一 Tracing（Jaeger）、Metrics（Prometheus）、Logging（Loki + Promtail），Grafana 一站式查询
- **单代码多角色**：所有节点运行同一份代码，通过环境变量配置不同角色（入口 / 中间 / 终端节点）
- **Docker Compose 一键启动**：包含 3 个 Janus 实例 + Nacos + etcd + Jaeger + Prometheus + Loki + Promtail + Grafana 的完整本地环境

## 技术栈

| 类别 | 技术 | 版本 |
|------|------|------|
| 语言 | Java | 25 |
| 构建 | Maven | 3.9+ |
| WebSocket | Java-WebSocket | 1.5.7 |
| gRPC | grpc-java | 1.82.1 |
| 序列化 | Protocol Buffers | 3.24.3 |
| JSON | Jackson | 2.18.2 |
| 服务发现 | Nacos / etcd (jetcd) | 3.2.2 / 0.8.6 |
| 可观测性 | OpenTelemetry | 1.63.0 |
| 追踪后端 | Jaeger | 1.76.0 |
| 指标 | Prometheus | v3.13.0 |
| 日志 | Loki + Promtail | 3.6.11 |
| 可视化 | Grafana | 12.2.10 |
| 日志框架 | Log4j2 | 2.26.1 |

## 快速开始

### 前置条件

- Docker 24+ & Docker Compose v2+
- Maven 3.9+ & JDK 25+（本地构建时需要）

### Docker Compose 启动

```bash
docker compose -f docker/docker-compose.yml --project-directory . up --build
```

启动后包含 10 个服务，等待所有服务健康后即可使用。

### 验证服务

```bash
docker compose -f docker/docker-compose.yml --project-directory . ps
docker compose -f docker/docker-compose.yml --project-directory . logs janus-server-I
```

启动成功后输出：

```
║ Janus Server started successfully
║   WebSocket:  ws://janus-server-I:8080/json  (JSON mode)
║   WebSocket:  ws://janus-server-I:8080/binary (Binary mode)
║   gRPC:       janus-server-I:9090
║   Metrics:    http://janus-server-I:9100/metrics
```

### 发送请求

使用 Postman 或任意 WebSocket 客户端连接 `ws://localhost:8080/json`，发送：

```json
{
  "data": "0",
  "meta": "postman"
}
```

请求经过完整链路：S1 (WS) → S2 (WS) → S3 (gRPC) → 本地处理 → 响应原路返回。

`data` 字段支持 0-5 的语言索引：

| data | 问候语 | 回应 |
|------|--------|------|
| 0 | Hello | Thank you very much |
| 1 | Bonjour | Merci beaucoup |
| 2 | Hola | Muchas Gracias |
| 3 | こんにちは | どうも ありがとう ございます |
| 4 | Ciao | Mille Grazie |
| 5 | 안녕하세요 | 대단히 감사합니다 |

## 架构

```
Postman ──[WS JSON/Binary]──▶ S1 ──[Nacos 发现]──▶ S2 ──[etcd 发现]──▶ S3
      (入口)                  (WS 转发)           (中间节点)           (gRPC 转发 → 本地处理)
```

| 节点 | 接收协议 | 转发协议 | 服务发现 | 服务注册 |
|------|---------|---------|---------|---------|
| S1 | WebSocket | WebSocket | Nacos | — |
| S2 | WebSocket | gRPC | etcd | Nacos |
| S3 | gRPC | — (本地处理) | — | etcd |

### 项目结构

```
src/main/java/org/janus/
├── JanusServer.java          # 主入口
├── Constants.java               # 常量定义
├── config/ServerConfig.java     # 环境变量配置
├── codec/BinaryCodec.java       # WS Binary 编解码
├── model/JanusMessage.java   # WS JSON 消息模型
├── common/HelloUtils.java       # 问候语工具
├── ws/                          # WebSocket 服务端 & 客户端
├── grpc/                        # gRPC 服务端 & 客户端 & 拦截器
├── discovery/                   # Nacos / etcd 注册发现 & gRPC NameResolver
├── observability/               # OpenTelemetry 初始化 & Span 工具
└── handler/ChainHandler.java    # 请求链路编排（协议转换 & 转发）
```

## 可观测性

三大信号统一接入 Grafana：

| 信号 | 采集 | 存储 | 查询 |
|------|------|------|------|
| Tracing | OpenTelemetry → OTLP | Jaeger | Jaeger UI / Grafana |
| Metrics | OpenTelemetry → Prometheus HTTP | Prometheus | Prometheus UI / Grafana |
| Logging | Log4j2 JSON → stdout → Promtail | Loki | Grafana LogQL |

### Grafana 统一查询

访问 `http://localhost:3000`（admin / admin），预配置三个数据源：

- **Loki** — `{service=~"janus-server.*"}` 查询日志
- **Prometheus** — `rate(rpc_calls_total[1m])` 查询指标
- **Jaeger** — 查看完整 Trace 链路 S1 → S2 → S3

支持数据源间交叉跳转：日志中的 `traceId` 可直接跳转 Jaeger Trace，Jaeger Trace 可跳转 Loki 日志。

## 本地构建

```bash
mvn clean package -DskipTests
```

构建产物：`target/janus.jar`（Fat JAR）

### 本地运行（单节点）

```bash
java -jar target/janus.jar
```

默认配置：WebSocket `ws://localhost:8080/json`、gRPC `localhost:9090`、Metrics `http://localhost:9100/metrics`

### 本地多节点链路

先启动基础设施：

```bash
docker run -d --name nacos -p 8848:8848 -p 9848:9848 \
  -e MODE=standalone \
  -e NACOS_AUTH_TOKEN=SmFudXNTZXJ2ZXJBdXRoVG9rZW4yMDI2U2VjcmV0S2V5 \
  -e NACOS_AUTH_IDENTITY_KEY=janus \
  -e NACOS_AUTH_IDENTITY_VALUE=janus \
  nacos/nacos-server:v3.2.2
docker run -d --name etcd -p 2379:2379 \
  -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
  -e ALLOW_NONE_AUTHENTICATION=yes quay.io/coreos/etcd:v3.6.13
docker run -d --name jaeger -p 16686:16686 -p 4317:4317 \
  -e COLLECTOR_OTLP_ENABLED=true jaegertracing/all-in-one:1.76.0
```

然后分别启动三个节点（使用 `export` 设置环境变量），详见 [使用指南](doc/guide.md#53-本地多节点链路运行)。

## 服务端口

| 服务 | 宿主机端口 | 用途 |
|------|-----------|------|
| janus-server-I | **8080** / 9091 / 9101 | WS 入口 / gRPC / Metrics |
| janus-server-II | 8081 / 9092 / 9102 | WS / gRPC / Metrics |
| janus-server-III | 8082 / 9093 / 9103 | WS / gRPC / Metrics |
| Nacos | **8848** | 控制台 |
| etcd | **2379** | API |
| Jaeger | **16686** | UI |
| Prometheus | **9090** | UI |
| Grafana | **3000** | UI (admin/admin) |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `JANUS_SERVER_ID` | janus-&lt;pid&gt; | 实例 ID（未设置时默认取进程 PID） |
| `JANUS_WS_PORT` | 8080 | WebSocket 端口 |
| `JANUS_GRPC_PORT` | 9090 | gRPC 端口 |
| `JANUS_METRICS_PORT` | 9100 | Prometheus 指标端口 |
| `JANUS_ADVERTISED_HOST` | localhost | 注册到发现中心的地址 |
| `JANUS_DOWNSTREAM_PROTOCOL` | none | 下游协议：`ws` / `grpc` / `none` |
| `JANUS_DOWNSTREAM_DISCOVERY` | none | 下游发现：`nacos` / `etcd` / `none` |
| `JANUS_DOWNSTREAM_SERVICE` | janus-server | 下游服务名 |
| `JANUS_REGISTER` | none | 注册中心：`nacos` / `etcd` / `none` |
| `JANUS_NACOS_ENDPOINT` | localhost:8848 | Nacos 地址 |
| `JANUS_ETCD_ENDPOINT` | http://localhost:2379 | etcd 地址 |
| `JANUS_OTEL_ENABLED` | Y | 是否启用 OpenTelemetry |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | http://localhost:4317 | OTLP 端点 |
| `OTEL_SERVICE_NAME` | janus | 服务名称（代码自动追加 `-{SERVER_ID}`） |
| `JANUS_AUTH_TOKEN` | （空） | 可选的 WebSocket 共享令牌鉴权。留空（默认）时不校验；设置后，入站 WS 握手必须携带匹配的 `authToken` 请求头，下游 WS 转发客户端也会自动带上该头（比较为常量时间，避免时序侧信道） |
| `JANUS_WS_POOL_SIZE` | 8 | 下游 WS 转发连接池大小。每条连接以 `request_id` 多路复用大量并发在途请求；连接按轮询分散到已发现的下游实例 |
| `JANUS_WS_FORWARD_TIMEOUT_MS` | 10000 | 单次下游 WS 往返超时（毫秒） |
| `JANUS_HANDLER_MAX_THREADS` | 512 | 处理线程池上限（仅在无虚拟线程的旧 JDK 回退时生效；JDK 21+ 使用虚拟线程） |
| `JANUS_GRPC_MAX_INBOUND_MSG` | 16777216 | gRPC 最大入站消息字节数 |
| `JANUS_GRPC_MAX_CONCURRENT_CALLS` | 0 | 单连接最大并发调用数（0 表示不限制） |
| `JANUS_GRPC_FLOW_WINDOW` | 8388608 | gRPC HTTP/2 流控窗口字节数（更大窗口提升高 BDP 链路吞吐） |
| `JANUS_GRPC_REFLECTION` | Y | 是否启用 gRPC 反射服务（便于 grpcurl；生产可设为 N 以减少信息暴露） |
| `JANUS_TLS_ENABLED` | N | 可选 TLS 总开关。启用后 gRPC 走 TLS、WebSocket 走 wss；默认关闭（演示栈保持明文）。配置错误时启动直接失败（fail-closed），不会静默降级为明文 |
| `JANUS_TLS_MTLS` | N | 是否要求双向 TLS（校验对端证书） |
| `JANUS_TLS_CERT` / `JANUS_TLS_KEY` / `JANUS_TLS_CA` | （空） | gRPC TLS 的 PEM 证书链 / PKCS8 私钥 / 信任根 |
| `JANUS_TLS_KEYSTORE` / `JANUS_TLS_KEYSTORE_PASSWORD` / `JANUS_TLS_KEYSTORE_TYPE` | （空）/（空）/ PKCS12 | WebSocket（wss）身份证书库 |
| `JANUS_TLS_TRUSTSTORE` / `JANUS_TLS_TRUSTSTORE_PASSWORD` | （空） | WebSocket 信任库（留空则用 JVM 默认信任库） |

## 文档

- [架构文档](doc/architecture.md) — 系统架构、模块设计、可观测性架构
- [使用指南](doc/guide.md) — 详细的使用说明、故障排查
- [协议规范](doc/protocol.md) — 统一消息信封、三种线路编码、协议转换规则

## 许可证

本项目仅供学习和演示用途。
