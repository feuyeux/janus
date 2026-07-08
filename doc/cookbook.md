# Janus Server —— 三服务全链路技术与运维详解

本文沿着一条真实请求在三个节点间的流动，把 Janus 的内部结构讲透。读完之后，下面几个问题应该都能自己回答：

- 三个 Janus 节点各由哪些组件构成，启动时如何装配，收到消息后如何流转？
- Nacos 与 etcd 两种注册发现，注册的 payload 如何**构造**、发现的 payload 如何**解析**、地址又如何**传递**给负载均衡器？
- WS JSON、WS Binary、gRPC 三种线路编码，各自的 payload 如何**解析**、如何**构造**、如何在节点之间**传递**并相互转换？

它与另外两篇文档定位不同，配合起来看效果最好：

- [`architecture.md`](architecture.md) 讲系统架构、模块清单和可观测性拓扑，回答"是什么"。
- [`protocol.md`](protocol.md) 是协议规范，给出三种线路编码的字段表与转换矩阵，回答"约定是什么"。
- 本文把前两者串成一条可以跟着源码逐行走读的路径，回答"它实际怎么跑、代码在哪"。

文中所有代码引用都指向 `src/main/java/org/janus/` 下的真实实现，行为描述以源码为准。

---

## 目录

1. [全景：三个节点是同一份代码的三种角色](#1-全景三个节点是同一份代码的三种角色)
2. [单节点内部组件全解剖](#2-单节点内部组件全解剖)
3. [统一消息模型 JanusMessage —— 一切的中心](#3-统一消息模型-janusmessage--一切的中心)
4. [注册与发现：Nacos 与 etcd 对照](#4-注册与发现nacos-与-etcd-对照)
5. [Payload 的解析、构造与传递](#5-payload-的解析构造与传递)
6. [全链路流程流转：把组件拼起来](#6-全链路流程流转把组件拼起来)
7. [Trace 上下文如何穿过三种协议](#7-trace-上下文如何穿过三种协议)
8. [四种 RPC 模型在全链路的真实行为](#8-四种-rpc-模型在全链路的真实行为)
9. [服务生命周期：启动、运行、停止](#9-服务生命周期启动运行停止)
10. [动手实践](#10-动手实践)
11. [排错对照表](#11-排错对照表)

---

## 1. 全景：三个节点是同一份代码的三种角色

Janus 有一个乍看反直觉的设计：**三个节点跑的是同一个 `janus.jar`**。入口、中间、终端这三种角色完全由环境变量决定，代码里没有任何 `if (isServer1)` 之类的分支。

### 1.1 角色由环境变量塑造

决定角色的只有三个变量（读取自 [`ServerConfig`](../src/main/java/org/janus/config/ServerConfig.java)）：

| 变量 | 含义 | 取值 |
|------|------|------|
| `JANUS_DOWNSTREAM_PROTOCOL` | 向下游转发时使用的协议 | `ws` / `grpc` / `none` |
| `JANUS_DOWNSTREAM_DISCOVERY` | 用哪个注册中心**发现**下游 | `nacos` / `etcd` / `none` |
| `JANUS_REGISTER` | 把自己**注册**到哪个注册中心 | `nacos` / `etcd` / `none` |
| `JANUS_REGISTER_PROTOCOL` | 向注册中心**公布的协议 / 端口**（上游以何协议访问本节点） | `ws` / `grpc` |

把 docker-compose 里三个节点的配置抽出来对照，角色分工一目了然：

| 节点 | 容器名 | DOWNSTREAM_PROTOCOL | DOWNSTREAM_DISCOVERY | REGISTER | REGISTER_PROTOCOL | 角色 |
|------|--------|---------------------|----------------------|----------|-------------------|------|
| S1 | `janus-server-i` | `grpc` | `nacos` | `none` | — | **入口**：收 WS JSON（`JANUS_WS_MODE=json`），用 Nacos 发现 S2，gRPC 转发 |
| S2 | `janus-server-ii` | `ws` | `etcd` | `nacos` | `grpc` | **中间**：收 gRPC，注册到 Nacos（以 gRPC 公布），用 etcd 发现 S3，WS **Binary** 转发（`JANUS_DOWNSTREAM_WS_MODE=binary`） |
| S3 | `janus-server-iii` | `none` | `none` | `etcd` | `ws` | **终端**：收 WS Binary（`JANUS_WS_MODE=binary`），注册到 etcd（以 WS 公布），本地处理 |

> 这里有几个容易忽略的要点：**注册和发现是两件独立的事，可分别用不同注册中心；公布协议与注册中心类型解耦；WS 服务端提供哪种线路编码（json/binary）也可收敛。**
> S2 同时做了两件事——注册到 Nacos（让 S1 经 gRPC 发现自己），又用 etcd 发现 S3（经 WS Binary 转发）。而 `JANUS_WS_MODE` 让 S1 只开 `/json`、S3 只开 `/binary`，`JANUS_DOWNSTREAM_WS_MODE=binary` 让 S2 的转发客户端连 S3 的 `/binary`。这正是本项目能同时演示两套注册发现、两种 WS 线路编码的原因。

### 1.2 数据在链路上怎么走

```
Postman ──WS(JSON)──▶ S1 ──gRPC──▶ S2 ──WS(Binary)──▶ S3 ──本地处理
   │                   │  ▲ Nacos发现S2  │  ▲ etcd发现S3   │
   │◀──────────────────┘◀───────────────┘◀───────────────┘
                          响应原路返回
```

- **两个协议转换点**：S1→S2 是 gRPC（S1 把入站 WS JSON 逻辑消息翻译成 gRPC 调用），S2→S3 是 **WS Binary**（S2 把入站 gRPC 调用翻译成一条 MSG_JANUS 二进制帧），见 [§5](#5-payload-的解析构造与传递) 与 [§6](#6-全链路流程流转把组件拼起来)。
- **两套发现分段生效**：S1 发现 S2 走 Nacos；S2 发现 S3 走 etcd。
- **响应原路返回**：每一跳都是“请求-等待-响应”的同步语义（即使内部用了异步 stub / 多路复用连接）。

### 1.3 启动装配顺序（JanusServer.start 的九步）

[`JanusServer.start()`](../src/main/java/org/janus/JanusServer.java) 是理解"一个节点由什么组成"的最佳入口。它严格按下面顺序装配，顺序本身就体现了依赖关系：

```
1. initOtel()            按 JANUS_OTEL_ENABLED / JANUS_METRICS_ENABLED 初始化 tracing 与 metrics，得到 TracingHelper
2. createRegistries()    按 REGISTER / DISCOVERY 建立注册中心连接（可能是两个不同实例）
3. new JanusServiceImpl / JanusGrpcServer   创建 gRPC 服务实现与服务端（尚未 start）
4. if grpc 下游: JanusGrpcClient.connect()  连下游 gRPC，并把 stub 注入 grpcService
5. new ChainHandler(tracingHelper, grpcClient)   创建链路编排器
6. if ws 下游: JanusWsClient.connect()      建立多路复用 WS 转发连接池，注入 ChainHandler
7. grpcServer.start(); wsServer.start()     两个监听器全部就绪
8. registerService()     两个监听器就绪后才注册自己（避免 peer 抢在 accept 前连入的启动竞态）
9. addShutdownHook()     注册优雅停机钩子
```

> 第 7、8 步的顺序是刻意的：**先监听、再注册**。源码注释直言这是为了避免"别人已经从注册中心发现了我、但我还没开始 accept 连接"的竞态。

这九步只是骨架。启动阶段真正值得深究的是"**同一份代码如何在运行时长成三种不同的角色**"——这部分放在 [§9.1](#91-启动同一份代码如何长成不同角色) 展开，与运行、停止两个阶段的工程细节合在一起讲。

---

## 2. 单节点内部组件全解剖

无论扮演哪种角色，每个节点装配的都是同一套组件，只是部分组件在特定角色下不会被激活。下面按职责逐一梳理。

### 2.1 组件清单

| 组件（类） | 包 | 职责 | 在哪些角色激活 |
|-----------|----|------|---------------|
| `JanusServer` | 根包 | 主入口，编排全部组件的生命周期 | 全部 |
| `ServerConfig` | `config` | 读取全部环境变量，暴露 `isXxx()` 判定方法 | 全部 |
| `Constants` | 根包 | gRPC Metadata Key、Context Key、WS 追踪头名 | 全部 |
| `JanusMessage` | `model` | **统一逻辑消息模型**（Java record），三协议共享 | 全部 |
| `BinaryCodec` | `codec` | WS Binary 帧编解码（`ByteWriter`/`ByteReader`/`encodeJanus`/`decodeJanus`） | 收/发 WS Binary 时 |
| `JanusWsServer` | `ws` | WS 服务端，`/json` 与 `/binary` 路由，收消息交给 ChainHandler | 收 WS 的节点（S1 入口、S3 终端） |
| `JanusWsClient` | `ws` | WS 转发客户端，**多路复用连接池** | WS 下游节点（S2） |
| `JanusGrpcServer` | `grpc` | gRPC 服务端（Netty），装拦截器、健康检查、反射 | 全部（都监听 gRPC） |
| `JanusServiceImpl` | `grpc` | gRPC 服务实现，4 种 RPC 模型；有 gRPC 下游 stub 则转发，配置 WS 下游则经 ChainHandler 走 WS 转发，否则本地处理 | 收 gRPC 的节点（S2；S1/S3 也监听但链路上不被打） |
| `JanusGrpcClient` | `grpc` | gRPC 转发客户端，按发现协议装配 Channel + NameResolver | gRPC 下游节点（S1） |
| `HeaderServerInterceptor` / `HeaderClientInterceptor` | `grpc` | 在 gRPC Metadata 里透传追踪头 | 收/发 gRPC 时 |
| `ChainHandler` | `handler` | **链路编排器**：解析入站 payload → 决定本地处理还是转发 → 协议转换 | 全部 |
| `NacosRegistry` / `EtcdRegistry` | `discovery` | 注册中心客户端，实现 `register/discover/deregister` | 按配置 |
| `NacosNameResolver` / `EtcdNameResolver` (+Provider) | `discovery` | 给 **gRPC 客户端**用的自定义 NameResolver | gRPC 下游 + 对应发现协议 |
| `OtelSupport` / `TracingHelper` | `observability` | OTel 初始化、Span 创建与上下文注入/提取 | 全部 |
| `HelloUtils` | `common` | 语言索引 → 问候语/回应（业务“终端处理”逻辑） | 本地处理时 |
| `ExecutorSupport` | `common` | 构造处理线程池（JDK21+ 虚拟线程，否则平台线程回退） | 全部 |

### 2.2 组件装配依赖图

```
                         JanusServer
                              │ 装配并持有
        ┌─────────────┬───────┴────────┬──────────────┬─────────────┐
        ▼             ▼                ▼              ▼             ▼
   TracingHelper  ServiceRegistry  JanusGrpcServer  JanusWsServer  (下游客户端)
   (OtelSupport)  (Nacos/Etcd)     └─JanusServiceImpl  └─┐         ┌─┴─────────┐
        │              │                                │      JanusGrpcClient  JanusWsClient
        │              │                                │      (NameResolver)  (多路复用池)
        │              │                                ▼             │             │
        └──────────────┴──────────────────────────▶ ChainHandler ◀───┴─────────────┘
                                                    (编排 + 协议转换)
                                                        │ 用到
                                                   BinaryCodec / JanusMessage / HelloUtils
```

`ChainHandler` 是所有入站路径的汇聚点：`JanusWsServer` 收到 JSON/Binary、`JanusServiceImpl` 收到 gRPC，最终都变成 `JanusMessage` 交给它编排。

### 2.3 线程模型（为什么 I/O 不会被阻塞）

- **WS 服务端**（`JanusWsServer`）：`onMessage` 只做一件事——把处理任务丢进 `handlerExecutor`，绝不在 WS 读线程上阻塞。因为处理可能会同步等一个下游 gRPC/WS 往返。
- **gRPC 服务端**（`JanusGrpcServer`）：`NettyServerBuilder.executor(handlerExecutor)` 把 RPC 处理放到独立线程池。
- **线程池来源**（`ExecutorSupport.newHandlerExecutor`）：JDK 21+（生产用 JDK 25）用 `newVirtualThreadPerTaskExecutor`（虚拟线程，海量并发阻塞任务成本极低）；老 JDK 回退到有界弹性平台线程池 + `AbortPolicy`（饱和时快速拒绝、返回 503/ERR，绝不把活儿内联到 I/O 线程上）。

---

## 3. 统一消息模型 JanusMessage —— 一切的中心

在讲 payload 之前，必须先认识 [`JanusMessage`](../src/main/java/org/janus/model/JanusMessage.java)。它是**唯一的逻辑消息**，三种线路编码都是它的“序列化外衣”。理解它，就理解了协议转换的本质：**换外衣，不换人**。

### 3.1 字段全表

`JanusMessage` 是一个不可变 `record`，`@JsonInclude(NON_NULL)`（null 字段不出现在 JSON 里）：

| 字段 | 类型 | JSON key | 语义 | 有效模式 |
|------|------|----------|------|----------|
| `method` | String | `method` | RPC 模型：`TALK` / `TALK_ONE_ANSWER_MORE` / `TALK_MORE_ANSWER_ONE` / `TALK_BIDIRECTIONAL` | 全部 |
| `mode` | String | `mode` | `REQUEST` / `RESPONSE` / `ERROR` | 全部 |
| `data` | String | `data` | 请求数据，语言索引 `"0"` 或逗号分隔 `"0,1,2"` | REQUEST |
| `meta` | String | `meta` | 客户端标识 | REQUEST |
| `status` | Integer | `status` | HTTP 风格状态码（200/500/503） | RESPONSE |
| `results` | List\<JanusResult\> | `results` | 结果列表 | RESPONSE |
| `errorMsg` | String | `error_msg` | 错误描述 | ERROR |
| `traceId` | String | `trace_id` | OpenTelemetry Trace ID | 全部 |
| `spanId` | String | `span_id` | OpenTelemetry Span ID | 全部 |
| `seq` | Integer | `seq` | 序列号（流式排序） | 全部 |
| `streamEnd` | Boolean | `stream_end` | 发送方半关闭标志 | 全部 |
| `requestId` | String | `request_id` | **每跳关联 id**，WS 多路复用用它把响应匹配回请求（WS JSON 为 JSON 字段，WS Binary 为 MSG_JANUS 帧字段） | 全部 |

`JanusResult`（嵌套 record）：

| 字段 | 类型 | JSON key | 语义 |
|------|------|----------|------|
| `id` | long | `id` | 唯一 id（代码里用 `System.nanoTime()`） |
| `type` | String | `type` | `OK` / `FAIL` |
| `kv` | Map\<String,String\> | `kv` | 键值对结果，含 `id`/`idx`/`data`/`meta` |

### 3.2 工厂方法与 wither（构造 payload 的入口）

`JanusMessage` 不用 setter，而是提供工厂方法和 wither（记录不可变，改一个字段就重建一个）：

```java
// 工厂：构造请求 / 响应 / 错误
JanusMessage.request(method, data, meta);                          // 默认 seq=0, streamEnd=true
JanusMessage.request(method, data, meta, traceId, spanId, seq, streamEnd);
JanusMessage.response(method, status, results);
JanusMessage.response(method, status, results, traceId, spanId, seq, streamEnd);
JanusMessage.error(method, errorMsg);

// wither：拷贝并只改一个字段（转发时链式使用）
msg.withRequestId(correlationId);   // 盖上关联 id
msg.withTrace(traceId, spanId);     // 换上本跳的 trace/span（WS 转发传播用）
```

### 3.3 method 与索引互转（跨编码的枢纽）

WS JSON 用字符串 `"TALK"`，WS Binary 用一个字节 `0x00`，gRPC 用方法名 `Talk`。三者靠 `JanusMessage` 里的两个方法互转：

```java
public int methodIndex();                     // "TALK_ONE_ANSWER_MORE" → 1
public static String methodFromIndex(int i);  // 1 → "TALK_ONE_ANSWER_MORE"
```

映射关系（与 proto 的 RPC 定义一一对应）：

| index | method 字符串 | gRPC 方法 | 通信模型 |
|-------|--------------|-----------|----------|
| 0 | `TALK` | `Talk` | Unary |
| 1 | `TALK_ONE_ANSWER_MORE` | `TalkOneAnswerMore` | Server Streaming |
| 2 | `TALK_MORE_ANSWER_ONE` | `TalkMoreAnswerOne` | Client Streaming |
| 3 | `TALK_BIDIRECTIONAL` | `TalkBidirectional` | Bidi Streaming |

这张映射表贯穿全篇：WS Binary 帧的第 0 字节、gRPC stub 的方法选择、本地处理的 `switch` 分支，判断依据都是它。

---

## 4. 注册与发现：Nacos 与 etcd 对照

这是全篇的重点之一。两套注册发现可以拆成四个环节对照来看：注册的 payload 如何构造、发现的 payload 如何解析、地址如何传递给调用方，以及各自有哪些容易踩的坑。

### 4.1 谁注册、谁发现

| 链路段 | 发现方 | 被发现方 | 注册中心 | 发现方式（代码路径） |
|--------|--------|----------|----------|---------------------|
| S1 → S2 | S1 | S2 | **Nacos** | `JanusGrpcClient` 用 `NacosNameResolver`（**gRPC 原生 NameResolver + subscribe**） |
| S2 → S3 | S2 | S3 | **etcd** | `JanusWsClient` 调 `EtcdRegistry.discover()`（**主动查询**，WS 连接池用） |

> **一个容易被文档误导、却很关键的事实**：在**默认 docker 拓扑**下，两套发现走的是**两种不同机制**，且与注册中心类型解耦——是由“**转发协议**”决定用哪种发现机制的。
> - Nacos 侧走 **gRPC 自定义 NameResolver**（`NacosNameResolver` + `subscribe`）：因为 S1→S2 是 **gRPC** 转发，地址交给 gRPC 框架的负载均衡器管理。
> - etcd 侧走 `ServiceRegistry.discover()`（`EtcdRegistry.discover`）：因为 S2→S3 是 **WS** 转发，由 `JanusWsClient` 主动拉取实例列表来填充连接池。
>
> 项目里同样实现了供 gRPC 使用的 `EtcdNameResolver`，但它只有在"grpc 下游 + etcd 发现"这个组合下才会被激活；默认链路里没有这个组合，因此它并不在默认链路的热路径上。下文两种发现用法都会覆盖，但默认链路实际跑的：Nacos 侧是 `NacosNameResolver`，etcd 侧是 `EtcdRegistry.discover`。

`ServiceRegistry` 接口（[`ServiceRegistry.java`](../src/main/java/org/janus/discovery/ServiceRegistry.java)）是两套实现的统一契约：

```java
void register(String serviceName, String host, int port, String protocol);
List<ServiceInstance> discover(String serviceName);
void deregister(String serviceName);
record ServiceInstance(String host, int port, String protocol) {}
```

### 4.2 注册端：JanusServer 如何决定“注册什么”

注册动作发生在 [`JanusServer.registerService()`](../src/main/java/org/janus/JanusServer.java)：

```java
// 公布协议/端口 = 上游以何协议访问本节点，与注册中心类型解耦，
// 由 JANUS_REGISTER_PROTOCOL 决定（默认：etcd→grpc，其余→ws）。
String protocol = ServerConfig.REGISTER_PROTOCOL;               // "ws" 或 "grpc"
int port = ServerConfig.registerAsGrpc() ? GRPC_PORT : WS_PORT; // 端口随协议
registry.register(SVC_DISC_NAME, ADVERTISED_HOST, port, protocol);
```

- 服务名统一是常量 `SVC_DISC_NAME = "janus-server"`。
- **S2 注册到 Nacos**：`(host=janus-server-ii, port=9090/gRPC, protocol=grpc)` —— 因为 S1 要用 gRPC 连它（`REGISTER_PROTOCOL=grpc`）。
- **S3 注册到 etcd**：`(host=janus-server-iii, port=8080/WS, protocol=ws)` —— 因为 S2 要用 WS 连它（`REGISTER_PROTOCOL=ws`）。

### 4.3 Nacos：注册 payload 构造 与 发现 payload 解析

实现见 [`NacosRegistry.java`](../src/main/java/org/janus/discovery/NacosRegistry.java)。

**连接构造**：只需一个 `SERVER_ADDR`。

```java
Properties p = new Properties();
p.put(PropertyKeyConst.SERVER_ADDR, endpoint);   // 如 "nacos:8848"
this.namingService = NacosFactory.createNamingService(p);
```

**注册 payload 构造**（`register`）：Nacos 的 payload 是一个 `Instance` 对象，`protocol` 放进 metadata：

```java
Instance instance = new Instance();
instance.setIp(host);            // janus-server-ii
instance.setPort(port);          // 9090（gRPC，因为 S1 经 gRPC 访问 S2）
instance.getMetadata().put("protocol", protocol);   // "grpc"
namingService.registerInstance(serviceName, instance);   // serviceName = "janus-server"
```

> Nacos 客户端在后台自动发送心跳维持实例健康，无需业务代码管理 TTL（对比 etcd 需要手动租约）。
> `deregister` 时用记录下来的 `registeredHost/registeredPort` 精确注销（而不是假设某个固定端口），代码里专门存了这两个字段。

**发现 payload 解析**（`discover`）：Nacos 直接返回结构化的 `Instance` 列表，解析很轻：

```java
List<Instance> instances = namingService.selectInstances(serviceName, true);  // healthy=true
for (Instance inst : instances) {
    String protocol = inst.getMetadata().getOrDefault("protocol", "ws");
    result.add(new ServiceInstance(inst.getIp(), inst.getPort(), protocol));
}
```

- `selectInstances(name, true)` 的 `true` 表示**只返回健康且启用的实例**，天然把宕掉的节点挡在负载均衡之外。
- 默认链路里 S1→S2 是 gRPC，因此这个方法是被 **`NacosNameResolver` 内部**调用来喂给 gRPC 负载均衡器的（见下）；`discover()` 本身则用于直连回退分支。

**gRPC 视角的 Nacos（`NacosNameResolver`，默认链路 S1→S2 实际走这条）**：当“grpc 下游 + nacos 发现”时（本项目 S1 的配置），gRPC 客户端不用 `discover()`，而是注册一个自定义 `NameResolver`（[`NacosNameResolver.java`](../src/main/java/org/janus/discovery/NacosNameResolver.java)）。它比一次性查询更强：

```java
namingService.subscribe(serviceName, event -> update());  // 订阅推送，成员变化免重启
// update() 内：
List<Instance> instances = namingService.selectInstances(serviceName, true);
List<EquivalentAddressGroup> groups = instances.stream()
        .map(i -> new EquivalentAddressGroup(new InetSocketAddress(i.getIp(), i.getPort())))
        .collect(toList());
if (groups.isEmpty()) {
    // 瞬时空列表（peer 刚注册、心跳还没标记健康）不清空 LB，保留 lastGood
    return;
}
lastGood = groups;
current.onAddresses(groups, Attributes.EMPTY);   // 把地址集推给 gRPC 负载均衡器
```

> 注意 `lastGood` 保护：发现结果瞬时为空时**不清空**负载均衡器地址集，避免一次抖动打断在途/新请求。这是生产级健壮性细节。

### 4.4 etcd：注册 payload 构造 与 发现 payload 解析

实现见 [`EtcdRegistry.java`](../src/main/java/org/janus/discovery/EtcdRegistry.java) 与 [`EtcdNameResolver.java`](../src/main/java/org/janus/discovery/EtcdNameResolver.java)。etcd 没有“服务实例”概念，只有 KV，所以注册/发现全靠**键值编码约定**。

**连接构造**：endpoint 需要 `http://` scheme，`normalizeEndpoint` 会补齐：

```java
this.endpoint = normalizeEndpoint(endpoint);   // "etcd:2379" → "http://etcd:2379"
this.etcd = Client.builder().endpoints(URI.create(this.endpoint)).build();
```

**注册 payload 构造**（`register`）——这是 etcd 与 Nacos 最不同的地方，靠 **Lease（租约）+ KeepAlive** 实现健康检查：

```java
long lease = etcd.getLeaseClient().grant(ServerConfig.ETCD_TTL).get().getID();  // TTL=10s

String uri = scheme + "://" + host + ":" + port;         // ws://janus-server-iii:8080（scheme 反映公布协议）
registeredKey = serviceName + "/" + uri;                 // janus-server/ws://janus-server-iii:8080
ByteSequence key   = ByteSequence.from(registeredKey, US_ASCII);
ByteSequence value = ByteSequence.from(protocol + "|" + lease, US_ASCII);  // "ws|7587321..."
etcd.getKVClient().put(key, value, PutOption.builder().withLeaseId(lease).build());

// 关键：把 KV 绑到租约上，并持续续租；进程一挂，续租停止，TTL 到期后 etcd 自动删除该 key
etcd.getLeaseClient().keepAlive(lease, new StreamObserver<>() { ... onNext: 续租成功 ... });
```

etcd 里最终存的一条记录长这样：

```
key   = janus-server/ws://janus-server-iii:8080
value = ws|7587321048576012345
        └┬┘ └────────┬─────────┘
      protocol      leaseId
```

> **注册 payload 的“构造”本质**：把 `host:port` 编码进 **key**（scheme 反映公布协议，如 `ws://` 或 `grpc://`），把 `protocol|leaseId` 编码进 **value**。健康检查不是靠心跳，而是靠**租约到期自动清除**——这是 etcd 服务发现的经典模式。gRPC 侧的 `EtcdNameResolver` 只从 key 里取 `host:port`（对 scheme 不敏感），WS 侧的 `discover()` 则从 value 读出 `protocol`——因此一个以 WS 公布的节点也能被正确存取。

**发现 payload 解析（两个解析器，别混淆）**：

1. `EtcdRegistry.discover()`（`ServiceRegistry` 接口实现，**默认链路 S2→S3 实际走这条**：由 `JanusWsClient` 主动拉取实例填充 WS 连接池）：前缀扫描 + 简单 substring 解析：

```java
GetResponse resp = etcd.getKVClient().get(prefix /*=serviceName*/, isPrefix(true)).get();
for (KeyValue kv : resp.getKvs()) {
    String keyStr = kv.getKey().toString(UTF_8);        // janus-server/ws://host:port
    String uriStr = keyStr.substring(keyStr.indexOf("/") + 1);  // ws://host:port
    URI uri = URI.create(uriStr);
    String valStr = kv.getValue().toString(US_ASCII);   // ws|lease
    String protocol = valStr.contains("|") ? valStr.split("\\|")[0] : "grpc";
    result.add(new ServiceInstance(uri.getHost(), uri.getPort(), protocol));
}
```

2. `EtcdNameResolver`（gRPC 用；仅在"grpc 下游 + etcd 发现"组合下激活，**默认链路里 S2→S3 是 WS，不走这条**）：初始全量 `get(prefix)` + `watch(prefix)` 增量维护：

```java
// 初始化：全量拉取当前在线实例
GetResponse query = kv.get(prefix, isPrefix(true)).get();
for (KeyValue kv : query.getKvs()) {
    String svcAddress = getUriFromDir(kv.getKey().toString(UTF_8));  // → <scheme>://host:port
    serviceUris.add(new URI(svcAddress));
}
updateListener();   // 推地址给 gRPC LB

// 增量：从上一次 revision 开始 watch，PUT 加地址 / DELETE 删地址
WatchOption opt = WatchOption.builder().withRevision(query.getHeader().getRevision()).build();
etcd.getWatchClient().watch(prefix, opt, this);   // onNext 里处理 PUT/DELETE 后再 updateListener()
```

其中 **key 解析函数 `getUriFromDir`** 是精髓（因为 key 里有 `://` 会干扰按 `/` 切分）：

```java
private static String getUriFromDir(String dir) {
    String tmp = dir.replace("://", "~");        // janus-server/grpc~host:port
    String[] tmps = tmp.split("/");              // ["janus-server", "grpc~host:port"]
    return tmps[tmps.length - 1].replace("~", "://");  // grpc~host:port → grpc://host:port
}
```

> **为什么要先把 `://` 换成 `~`？** 因为 key 是 `janus-server/grpc://host:port`，直接按 `/` 切会把 `grpc://` 里的双斜杠切碎。先临时替换 `://` 为 `~` 保护它，切完取最后一段，再换回来。源码注释特别强调：watch 事件解析必须和初始加载用**完全相同**的剥前缀逻辑，否则 `grpc://host:port` 里的 host 会被解析成 `null`。

**地址如何“传递”给 gRPC 负载均衡器**：`updateListener()` 把每个 URI 变成 `EquivalentAddressGroup` 列表，调用 `listener.onAddresses(list, Attributes.EMPTY)`，gRPC 的 `round_robin` LB 就据此在多个 S3 实例间轮询。

### 4.5 gRPC 客户端如何选发现协议并装配 Channel

[`JanusGrpcClient.connect()`](../src/main/java/org/janus/grpc/JanusGrpcClient.java) 根据 `DOWNSTREAM_DISCOVERY` 决定用哪个 NameResolver：

```java
if (isEtcdDiscovery()) {
    String target = "etcd:///" + DOWNSTREAM_SERVICE;    // 注意三斜杠：etcd:///janus-server
    NameResolverRegistry.getDefaultRegistry().register(
        EtcdNameResolverProvider.forEndpoints(List.of(URI.create("http://etcd:2379"))));
    builder = NettyChannelBuilder.forTarget(target).defaultLoadBalancingPolicy("round_robin");
} else if (isNacosDiscovery()) {
    String target = "nacos://" + DOWNSTREAM_SERVICE;
    NameResolverRegistry.getDefaultRegistry().register(
        new NacosNameResolverProvider(URI.create("nacos://" + NACOS_ENDPOINT)));
    builder = NettyChannelBuilder.forTarget(target).defaultLoadBalancingPolicy("round_robin");
} else {
    // 直连回退：用 registry.discover() 拿第一个实例地址
    ServiceInstance inst = registry.discover(DOWNSTREAM_SERVICE).get(0);
    builder = NettyChannelBuilder.forAddress(inst.host(), inst.port());
}
```

- `etcd:///janus-server` 的 scheme `etcd` 触发 `EtcdNameResolverProvider`，路径 `/janus-server` 去掉首斜杠成为 `serviceDir`（`EtcdNameResolverProvider` 里断言路径必须以 `/` 开头）。
- 两个 Provider 都 `priority()=6`、`isAvailable()=true`，通过 `NameResolverRegistry` 注册后由 scheme 匹配。
- Channel 上还挂了 keepAlive、`enableRetry()`、`HeaderClientInterceptor`（透传追踪头）和 OTel 客户端拦截器。

---

## 5. Payload 的解析、构造与传递

同一个 `JanusMessage`，套着三件不同的外衣。这一节分别看它在每种编码下如何被解析成 `JanusMessage`、如何从 `JanusMessage` 构造回字节，以及在 `ChainHandler` 里如何转换和传递。

### 5.1 WS JSON（`/json`）

**解析（入站）**：`JanusWsServer.onMessage(conn, String)` 收到文本帧，交给 `ChainHandler.handleJsonRequest`，用 Jackson 反序列化：

```java
private static final ObjectMapper objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);  // 容忍未知字段
request = objectMapper.readValue(jsonRequest, JanusMessage.class);
```

> `FAIL_ON_UNKNOWN_PROPERTIES=false` 很关键：下游返回的错误信封或未来新增字段不会让某一跳解析崩溃。

**构造（出站）**：`objectMapper.writeValueAsString(response)`。因为 `@JsonInclude(NON_NULL)`，`REQUEST` 里的 `status/results` 等 null 字段不会出现，`RESPONSE` 里的 `data/meta` 也不会出现。一条典型请求/响应：

```json
// 请求
{"method":"TALK","mode":"REQUEST","data":"0","meta":"postman","seq":0,"stream_end":true}
// 响应
{"method":"TALK","mode":"RESPONSE","status":200,
 "results":[{"id":123,"type":"OK","kv":{"idx":"0","data":"Hello,Thank you very much","meta":"postman"}}],
 "seq":0,"stream_end":true,"request_id":"<corr>"}
```

**传递**：WS 文本帧不能带 header，所以 `request_id`（关联 id）和 `trace_id/span_id` 都**编码在 JSON 正文里**，随消息一起走。响应里回显收到的 `request_id`（`response.withRequestId(request.requestId())`），让上游多路复用客户端能把响应对上号。

### 5.2 WS Binary（`/binary`，MSG_JANUS = 0x10）

这是三种编码里最“底层”的一种，需要手工读写字节。核心是 [`BinaryCodec`](../src/main/java/org/janus/codec/BinaryCodec.java)。

**帧结构**：8 字节头 + 载荷。

```
偏移  长度  字段         值/说明
0     1    MAGIC        0x48 ('H')
1     1    VERSION      0x01
2     1    MSG_TYPE     0x10 = MSG_JANUS（统一消息，承载全部 4 种 RPC）
3     1    FLAGS        0x00（保留）
4     4    PAYLOAD_LEN  uint32 大端序
8     N    PAYLOAD      见下
```

**MSG_JANUS 载荷的构造顺序**（`encodeJanus`，这是权威顺序，逐字段）：

```java
w.writeU8(method);        // 0=TALK,1=ONE_ANSWER_MORE,2=MORE_ANSWER_ONE,3=BIDIRECTIONAL
w.writeU8(mode);          // 0=REQUEST,1=RESPONSE,2=ERROR
w.writeI32(seq);
w.writeU8(streamEnd?1:0);
w.writeI32(status);
w.writeString(data);      // string = u32 长度 + UTF-8 字节
w.writeString(meta);
w.writeString(traceId);
w.writeString(spanId);
w.writeString(errorMsg);
w.writeString(requestId);         // 每跳关联 id：二进制多路复用转发靠它匹配回复（空串=未用）
w.writeU32(results.length);       // 无结果则写 0
for (r : results) { w.writeI64(r.idx); w.writeU8(r.type); w.writeKV(r.kv); }
```

**载荷的解析顺序**（`decodeJanus`）与构造**严格镜像**——`readU8 / readU8 / readI32 / readU8 / readI32 / readString ×6 / readCount / (readI64 / readU8 / readKV)*`（`readString ×6` = data, meta, traceId, spanId, errorMsg, **requestId**）。

**基本类型编解码**（`ByteWriter` / `ByteReader`）：

| 类型 | 编码 | 读方法 |
|------|------|--------|
| u8 | 1 字节 | `readU8` |
| u32 | 4 字节大端 | `readU32` |
| i32 | 4 字节大端有符号 | `readI32` |
| i64 | 8 字节大端 | `readI64` |
| string | u32 长度 + UTF-8 | `readString` |
| kv | u32 条目数 + (string key + string value)* | `readKV` |

**安全边界（构造 payload 时必须理解的防御）**：`ByteReader.readCount()` 对所有“长度/数量”前缀做双重校验：

```java
if (count > MAX_ELEMENTS /*1<<20*/) throw new DecodeException(...);  // 防超大分配
if (count > data.length - pos)      throw new DecodeException(...);  // 防超出剩余字节
```

> 这两条校验挡住了恶意帧用一个伪造的巨大 count 触发 OOM 或 `NegativeArraySizeException`。手工构帧时如果 count 与实际字节不符，会被这里拒绝。

**入站分发**：`JanusWsServer.onBinaryMessage` 先看第 2 字节：

```java
if (data[2] == BinaryCodec.MSG_JANUS) {          // 0x10 → 统一消息路径
    BinaryCodec.JanusFrame frame = BinaryCodec.decodeJanus(data);
    conn.send(chainHandler.handleBinaryJanus(frame));
} else {                                          // 其余走经典消息（HELLO/PING/ECHO...）
    BinaryCodec.Message msg = BinaryCodec.decodeMessage(data);
    switch (msg.type) { MSG_HELLO → 回 BONJOUR; MSG_PING → 回 PONG; MSG_ECHO_REQUEST → ...; ... }
}
```

`onOpen` 时若路径是 `/binary`，服务端会主动先发一帧 `BONJOUR("JANUS-JAVA")` 作为握手。

**帧 → JanusMessage（解析成逻辑消息）**：`ChainHandler.handleBinaryJanus` 把 `JanusFrame` 装成 `JanusMessage`：

```java
String methodName = JanusMessage.methodFromIndex(frame.method());   // 0 → "TALK"
JanusMessage request = new JanusMessage(methodName,
        frame.mode()==0 ? MODE_REQUEST : MODE_RESPONSE,
        frame.data(), frame.meta(), null,null,null,
        frame.traceId(), frame.spanId(), frame.seq(), frame.streamEnd(), null);
```

**JanusMessage → 帧（构造出站字节）**：`ChainHandler.envelopeToBinary` 把逻辑消息（含 results）编回 MSG_JANUS：

```java
int modeIdx = switch(msg.mode()) { REQUEST→0; ERROR→2; default→1; };
// results 转成 BinaryCodec.EchoResult[]（id/type/kv）
return BinaryCodec.encodeJanus(msg.methodIndex(), modeIdx, seq, streamEnd,
        status, data, meta, traceId, spanId, errorMsg, results);
```

> 注意：Binary 内部复用了 `EchoResult(long idx, int type, Map kv)` 这个 record 来承载结果（`type`：0=OK，1=FAIL）。

### 5.3 gRPC（Protocol Buffers）

proto 定义见 [`janus.proto`](../src/main/proto/janus.proto)，生成类在 `org.janus.proto` 包（`TalkRequest` / `TalkResponse` / `TalkResult` / `ResultType` / `JanusServiceGrpc`）。

**构造请求（出站，WS→gRPC 转换时）**：`ChainHandler.forwardViaGrpc` 用 builder 构造 `TalkRequest`：

```java
TalkRequest req = TalkRequest.newBuilder()
        .setData(request.data() != null ? request.data() : "0")
        .setMeta(request.meta() != null ? request.meta() : "JAVA")
        .setTraceId(traceId).setSpanId(spanId)     // trace 编进消息体
        .setSeq(...).setStreamEnd(...)
        .build();
```

**选择 RPC 方法（传递）**：按 `method` 选 stub 方法：

```java
switch (request.method()) {
    case TALK_ONE_ANSWER_MORE -> {   // server streaming：遍历迭代器收集所有响应
        Iterator<TalkResponse> it = blockingStub.talkOneAnswerMore(req);
        while (it.hasNext()) { allResults.addAll(...); }
    }
    case TALK_MORE_ANSWER_ONE, TALK_BIDIRECTIONAL -> blockingStub.talk(req);  // 见 §8：桥接降级为 unary
    default -> blockingStub.talk(req);   // unary
}
```

**响应解析（gRPC → JanusMessage）**：`grpcResponseToEnvelope` + `talkResultsToEnvelope`：

```java
List<JanusResult> results = new ArrayList<>();
for (TalkResult r : resp.getResultsList()) {
    results.add(new JanusResult(r.getId(),
            r.getType()==ResultType.OK ? "OK" : "FAIL",
            new HashMap<>(r.getKvMap())));
}
return JanusMessage.response(method, resp.getStatus(), results, traceId, spanId, resp.getSeq(), resp.getStreamEnd());
```

**追踪头传递**：gRPC 请求体带了 `trace_id/span_id`，同时 `HeaderClientInterceptor` 会把 gRPC `Context` 里的追踪 key 复制到 Metadata（见 §7）。

### 5.4 协议转换矩阵在代码里的落点

| 转换 | 代码位置 | 方向 |
|------|----------|------|
| JSON → JanusMessage | `handleJsonRequest` → `objectMapper.readValue` | 入站解析 |
| JanusMessage → JSON | `handleJsonRequest` → `writeValueAsString` | 出站构造 |
| Binary 帧 → JanusMessage | `handleBinaryJanus`（入站）/ `binaryFrameToEnvelope`（转发回复） | 入站解析 |
| JanusMessage → Binary 帧 | `envelopeToBinary` → `encodeJanus` | 出站构造 |
| JanusMessage → TalkRequest | `forwardViaGrpc` | 出站构造（S1：WS→gRPC 转发） |
| TalkResponse → JanusMessage | `grpcResponseToEnvelope` / `talkResultsToEnvelope` | 入站解析（S1 收 gRPC 响应） |
| TalkRequest → JanusMessage | `handleGrpcRequest` | 入站解析（S2：gRPC 入口） |
| JanusMessage → TalkResponse | `envelopeToTalkResponse` | 出站构造（S2 回 gRPC 响应） |
| JanusMessage → 下游 WS（本链路 Binary 帧） | `forwardViaWs`（按 `isBinary()` 走 `envelopeToBinary`/JSON） | 传递（S2：gRPC→WS 转发） |

---

## 6. 全链路流程流转：把组件拼起来

现在把前面所有零件拼成一条完整的请求生命线。以最经典的一条为例：

> **Postman 用 WS JSON 发 `{"method":"TALK","mode":"REQUEST","data":"0","meta":"postman"}` 到 S1。**

### 6.1 逐跳时序 + payload 快照

```
Postman                 S1 (入口)              S2 (中间)               S3 (终端)
  │  WS JSON REQUEST       │                      │                      │
  ├──────────────────────▶│ onMessage(text)      │                      │
  │                        │ handleJsonRequest    │                      │
  │                        │ 解析→JanusMessage    │                      │
  │                        │ startServerSpan      │                      │
  │                        │ route(): grpc 下游   │                      │
  │                        │ forwardViaGrpc       │                      │
  │                        │ 构造 TalkRequest     │                      │
  │                        │ gRPC Talk()          │                      │
  │                        ├─────────────────────▶│ JanusServiceImpl.talk│
  │                        │  (Nacos 发现的 S2)   │ wsForward()==true    │
  │                        │                      │ handleGrpcRequest    │
  │                        │                      │ TalkRequest→Envelope │
  │                        │                      │ startServerSpan      │
  │                        │                      │ route(): ws 下游     │
  │                        │                      │ forwardViaWs(binary) │
  │                        │                      │ 生成 correlationId   │
  │                        │                      │ envelopeToBinary     │
  │                        │                      │ WS Binary REQUEST    │
  │                        │                      ├─────────────────────▶│ onMessage(bytes)
  │                        │                      │  (etcd 发现的 S3)    │ onBinaryMessage
  │                        │                      │                      │ decodeJanus(取 reqId)
  │                        │                      │                      │ route(): 本地处理
  │                        │                      │                      │ HelloUtils.getGreeting("0")
  │                        │                      │                      │ = "Hello"/"Thank you very much"
  │                        │                      │◀─────────────────────┤ WS Binary RESPONSE
  │                        │                      │ 收到 reply(回显 reqId)│
  │                        │                      │ decodeJanus→Envelope │
  │                        │                      │ Envelope→TalkResponse│
  │                        │◀─────────────────────┤ gRPC TalkResponse    │
  │                        │ grpcResponseToEnvelope│ {status:200,results}│
  │◀───────────────────────┤ WS JSON RESPONSE     │                      │
  │  {status:200,results}  │ (回显 request_id)     │                      │
```

每一跳的 payload 形态：

| 跳 | 线路编码 | payload 关键内容 |
|----|----------|------------------|
| Postman→S1 | WS JSON | `{"method":"TALK","mode":"REQUEST","data":"0","meta":"postman"}` |
| S1→S2 | gRPC | `TalkRequest{data:"0", meta:"postman", trace_id, span_id（S1 client span）, seq:0, stream_end:true}` |
| S2→S3 | WS **Binary** | `MSG_JANUS(method=0, mode=0, data="0", meta="postman", trace_id, span_id（S2 client span）, request_id=<S2 生成的 correlationId>)` |
| S3→S2 | WS **Binary** | `MSG_JANUS(mode=1, status=200, results=[{idx:"0",data:"Hello,Thank you very much",meta:"postman"}], request_id=<S2的corr>)` |
| S2→S1 | gRPC | `TalkResponse{status:200, results:[...], seq:0, stream_end:true}` |
| S1→Postman | WS JSON | `{"method":"TALK","mode":"RESPONSE","status":200,"results":[...]}`（S1 回显它收到的 request_id；Postman 没设则为 null，被 NON_NULL 省略） |

### 6.2 WS→gRPC 转发（S1 内部）

`ChainHandler.forwardViaGrpc` 是入口节点的协议转换点：把入站 WS 逻辑消息翻译成一次 gRPC 调用。

- `route()` 判定 `isGrpcDownstream() && grpcClient.isConnected()` → 走 gRPC。若 `isGrpcDownstream()` 为真但连接不可用，`route()` **不回退到本地处理**，而是返回 `status=503` 的 `ERROR` 信封（本地处理仅保留给未配置下游的终端节点），避免转发节点用伪造的本地结果掩盖下游故障。
- 起一个 **client span**（`grpc-forward-<method>`），把它的 `traceId/spanId` 塞进 `TalkRequest`，实现跨协议 trace 续接；`HeaderClientInterceptor` 再把 gRPC `Context` 的追踪 key 复制进 Metadata（见 §7）。
- 按 `method` 选 stub 方法（unary / server-streaming），对 client-streaming 与 bidi **降级为 unary**（原因见 §8）。
- 下游地址由 `NacosNameResolver`（gRPC 原生 resolver）解析，交给 `round_robin` LB。

### 6.3 gRPC 接收 → WS 转发：多路复用与关联 id（S2 内部）

S2 是**双重转换点**：`JanusServiceImpl` 收到 gRPC 调用后，因配置了 WS 下游，走 `wsForward()` 分支调用 `ChainHandler.handleGrpcRequest`，它把 `TalkRequest` 升为统一信封、`route()` 判定 WS 下游、再由 `forwardViaWs` + `JanusWsClient` 经**多路复用连接池**转发到 S3：

```java
// JanusServiceImpl.talk（S2：blockingStub==null 且 isWsDownstream()）
response = chainHandler.handleGrpcRequest(JanusMessage.METHOD_TALK, request);
// ChainHandler.handleGrpcRequest → route() → forwardViaWs（本链路 binary 分支）
String correlationId = UUID.randomUUID().toString();
JanusMessage outbound = request.withTrace(traceId, spanId).withRequestId(correlationId);
if (wsClient.isBinary()) {                                  // JANUS_DOWNSTREAM_WS_MODE=binary
    byte[] reqBytes  = envelopeToBinary(outbound);          // MSG_JANUS 帧，request_id 写入帧
    byte[] respBytes = wsClient.forwardBinary(correlationId, reqBytes);
    return binaryFrameToEnvelope(BinaryCodec.decodeJanus(respBytes));
} else {                                                    // json 模式
    String jsonResponse = wsClient.forward(correlationId, objectMapper.writeValueAsString(outbound));
    return objectMapper.readValue(jsonResponse, JanusMessage.class);
}
// 返回的 JanusMessage 再由 envelopeToTalkResponse 转回 gRPC 调用方
```

`JanusWsClient` 的关键机制（[`JanusWsClient.java`](../src/main/java/org/janus/ws/JanusWsClient.java)）：

- **单一线路编码**：整个连接池由 `JANUS_DOWNSTREAM_WS_MODE` 决定连 `/binary`（MSG_JANUS 帧）还是 `/json`（文本帧）；本链路 S2→S3 是 `binary`（因为 S3 只提供 `/binary`）。
- **连接池**：`WS_POOL_SIZE`（默认 8）条到下游的长连接，每条连接用 `request_id` 多路复用**大量在途请求**。地址来自 `registry.discover(downstreamService)`（**etcd**），按轮询分散到已发现实例。
- **发送-等待-匹配**：`MultiplexClient.roundTrip` 先把一个 `CompletableFuture` 以 `correlationId` 为 key 放进 `pending` map，再 `send(bytes|json)`；下游回复到 `onMessage` 时——JSON 从文本抽 `request_id`，Binary 用 `decodeJanus(bytes).requestId()`——`pending.remove(corr).complete(reply)` 唤醒等待者。二进制帧的 `request_id` 字段就是为此新增的。
- **热路径不阻塞**（R4）：`forward` 只挑已打开的连接；(重)连接和发现都在后台单线程 `maintainer` 上做（每 3 秒补池），下游故障时请求**快速失败**而不是让大量 handler 线程串行卡在重连上。
- **安全重试**（R1）：只有当 `send` 本身抛异常（字节根本没上线）才在另一条连接重试；一旦字节已发出，即使中途失败也**不重发**（避免下游对非幂等请求重复处理）。
- **超时**：`WS_FORWARD_TIMEOUT_MS`（默认 10s），`future.get(timeout)` 超时抛错但不重发。
- **流式降级**：因 WS 往返一次只承载一条逻辑消息，client-streaming / bidi 在此收敛为一元往返（见 §8）。
- **BONJOUR 忽略**：连上 `/binary` 时 S3 会先回一帧 BONJOUR（非 MSG_JANUS），`onMessage(ByteBuffer)` 检测帧类型后静默丢弃，不影响关联。

> 关键点：**每个节点都盖上它自己收到的 `request_id` 回响应**（`response.withRequestId(request.requestId())`，二进制路径在 `handleBinaryJanus` 里做）。因此关联是“每段本地”的，能在 S1→S2→S3 之间正确嵌套——S2 的池用 S2 生成的 corr 匹配，S3 收到什么就回什么。

### 6.4 WS Binary 接收 → 本地处理（S3 内部）

S3 收 WS Binary（`JANUS_WS_MODE=binary`，只开 `/binary`），本地处理走 `JanusWsServer.onBinaryMessage` → `decodeJanus` → `ChainHandler.handleBinaryJanus` → `route()`（无下游）→ `processLocally`；`handleBinaryJanus` 会把收到的 `request_id` 回写到响应帧：

```java
// ChainHandler.handleBinaryJanus（S3：无下游 → 终端处理）
JanusMessage request  = /* 从 JanusFrame 构造，携带 frame.requestId() */;
JanusMessage response = route(request, ...);        // → processLocally
response = response.withRequestId(frame.requestId());   // 回显关联 id
return envelopeToBinary(response);                  // request_id 写回帧
// processLocally：按 method 分派；unary/bidi/client-streaming 返回单条结果，
// server-streaming 按逗号拆分 data 返回多条
```

`createResult` 就是业务终点（[`HelloUtils`](../src/main/java/org/janus/common/HelloUtils.java)）：

```java
String greeting = HelloUtils.getGreeting("0");   // 索引 0 → "Hello"
String answer   = HelloUtils.getAnswer("Hello"); // → "Thank you very much"
kv = {id: <uuid>, idx: "0", data: "Hello,Thank you very much", meta: "postman"}
```

语言索引表：

| data | 问候 (greeting) | 回应 (answer) |
|------|-----------------|---------------|
| 0 | Hello | Thank you very much |
| 1 | Bonjour | Merci beaucoup |
| 2 | Hola | Muchas Gracias |
| 3 | こんにちは | どうも ありがとう ございます |
| 4 | Ciao | Mille Grazie |
| 5 | 안녕하세요 | 대단히 감사합니다 |

> 越界或非数字的 `data` 一律回退为索引 0（"Hello"）。

### 6.5 响应原路返回

S3 的 `JanusMessage` 响应 → `envelopeToBinary` 编成 MSG_JANUS 帧（回写 `request_id`）回给 S2 的多路复用连接 → S2 的 `onMessage(ByteBuffer)` 用帧内 `request_id` 匹配唤醒 `forwardBinary`，`binaryFrameToEnvelope` 转回 `JanusMessage`，再经 `envelopeToTalkResponse` 转成 `TalkResponse` 回给 S1 的 gRPC 调用 → S1 用 `grpcResponseToEnvelope` 转回 `JanusMessage` → 序列化为 WS JSON 回给 Postman。每一跳 `finally` 里 `endSpan` 结束自己的 span。

---

## 7. Trace 上下文如何穿过三种协议

全链路可观测的核心难点：**WS 和 gRPC 的上下文载体完全不同**，但要串成一条 trace。[`TracingHelper`](../src/main/java/org/janus/observability/TracingHelper.java) + [`OtelSupport`](../src/main/java/org/janus/observability/OtelSupport.java) 解决这个问题。

### 7.1 每跳两个 span：server span + client span

- 入站时 `startServerSpan(name, ctx)`：从 carrier（Map）里 `extract` 父上下文，创建 `SERVER` span，**再把这个 server span 注入回 carrier**。
- 转发时 `startClientSpan(name, ctx)`：从**同一个 carrier** `extract`，于是它成为刚才 server span 的**子 span**（而不是兄弟），并把 client span 注入 carrier 传给下游。

```java
// startServerSpan 的关键三步
Context parent = propagator.extract(Context.current(), traceContext, mapGetter());
Span span = tracer.spanBuilder(name).setParent(parent).setSpanKind(SERVER).startSpan();
Context withSpan = span.storeInContext(parent);
propagator.inject(withSpan, traceContext, mapSetter());   // ★ 回写 carrier，保证下游 span 挂在它下面
MDC.put("traceId", ...); MDC.put("spanId", ...);          // 让日志带上 traceId/spanId
```

> 这个“server span 回写 carrier”的动作是链路正确嵌套的关键：在没有入站上下文的入口节点（S1），它防止下游调用另起一条断裂的新 trace。

### 7.2 WS 文本帧带不了 header：用信封里的 trace_id 重建

WS 文本帧无法附加 header，因此上下文只能**编码在 JSON 信封字段**里。当 carrier 为空时，`ChainHandler.mergeTraceContext` 会用信封自带的 `trace_id/span_id` 拼出一个 W3C `traceparent`：

```java
if (!ctx.containsKey("traceparent") && req.traceId() != null && !req.traceId().isEmpty()) {
    String spanId = (req.spanId()!=null && !req.spanId().isEmpty()) ? req.spanId() : "0000000000000000";
    ctx.put("traceparent", "00-" + req.traceId() + "-" + spanId + "-01");
}
```

WS Binary 同理，`handleBinaryJanus` 用 `frame.traceId()/spanId()` 拼 `traceparent`。这样 **WS 转发跳（如 S2→S3）仍在同一条 trace 上**。

### 7.3 gRPC 用 Metadata 传递

gRPC 有原生 Metadata，走两条并行通道：

1. **OTel 自带**：`OtelSupport.grpcClientInterceptor()/grpcServerInterceptor()`（`GrpcTelemetry`）自动做标准 W3C 传播。
2. **项目自定义头**（[`HeaderClientInterceptor`](../src/main/java/org/janus/grpc/HeaderClientInterceptor.java) / `HeaderServerInterceptor`）：把 gRPC `Context` 里的 `x-request-id/x-trace-id/x-span-id` 复制进/出 Metadata。

> 注意源码里的一个防御性修正：拦截器只遍历 `CONTEXT_KEYS.size()`（3）个 key，而不是 `TRACING_KEYS.size()`（7）。因为只有前 3 个追踪 key 有对应的 Context key，若按 7 遍历，遇到 b3 头会数组越界，导致**每个 gRPC 调用都中止**。这是 `Constants` 里两个列表长度不一致埋下的坑，拦截器用取小的那个长度来规避。

### 7.4 trace_id 一路带进日志

`TracingHelper` 在起 span 时 `MDC.put("traceId"/"spanId")`，Log4j2 输出 JSON 日志时带上它们；Promtail 把 `traceId` 提为 Loki 标签，于是能从日志一键跳到 Jaeger 的对应 trace（见 `architecture.md` §6）。

### 7.5 完整 trace 的样子

```
Trace（一个 traceId 贯穿）
├─ S1 ws-json-TALK           (SERVER)
│   └─ S1 grpc-forward-TALK   (CLIENT)      ← 注入到 TalkRequest + gRPC Metadata
│       └─ S2 grpc-TALK       (SERVER)      ← handleGrpcRequest 建 server span（+ OTel GrpcTelemetry）
│           └─ S2 ws-forward-TALK (CLIENT)  ← 注入到 WS Binary 帧 trace_id/span_id
│               └─ S3 ws-binary-TALK (SERVER) ← 从二进制帧重建父上下文
```

---

## 8. 四种 RPC 模型在全链路的真实行为

这一节要澄清一个很容易误解的点：四种 RPC 模型在"**原生 gRPC 直连**"和"**桥接到异构传输**"两条路径上的行为并不一致。默认链路里有**两个桥接点**：S1（WS 入口 → gRPC 转发，`forwardViaGrpc`）与 S2（gRPC 入口 → WS 转发，`handleGrpcRequest` + `forwardViaWs`）。

### 8.1 原生 gRPC 直连（gRPC → gRPC 同协议转发）

`JanusServiceImpl` 完整实现了四种模型的真实流式语义（仅当配置了 gRPC 下游 stub 时）：

| 方法 | 模型 | 无下游（本地）行为 | 有 gRPC 下游（转发）行为 |
|------|------|--------------------|--------------------|
| `talk` | Unary | 一个请求→一个响应 | `blockingStub.talk` 透传 |
| `talkOneAnswerMore` | Server streaming | 把 `data` 按逗号拆分，逐条 `onNext`，最后一条 `streamEnd=true` | 迭代下游响应逐条 `onNext` |
| `talkMoreAnswerOne` | Client streaming | 返回一个 `StreamObserver`，累积每条 `onNext` 的结果，`onCompleted` 时汇总成一个响应 | 建立到下游的双向 observer 转发每条请求 |
| `talkBidirectional` | Bidi streaming | 每收到一条请求就即刻回一条响应 | 双向 observer 全透传 |

### 8.2 桥接到异构传输：流式被“压扁”为一元往返

两个桥接点都因“一次只承载一条逻辑消息”而将 client-streaming / bidi **刻意降级为一次一元往返**：

- **S1（WS → gRPC，`forwardViaGrpc`）**：

```java
case TALK_MORE_ANSWER_ONE -> blockingStub.talk(grpcRequest);   // 注释：collapsed to unary
case TALK_BIDIRECTIONAL   -> blockingStub.talk(grpcRequest);   // 注释：collapsed to unary
```

- **S2（gRPC → WS，`JanusServiceImpl` 的 `wsForward()` 分支 → `handleGrpcRequest` → `forwardViaWs`）**：client-streaming 累积后在 `onCompleted` 做一次 WS 往返，bidi 每条请求做一次 WS 往返，server-streaming 聚合为**单个响应帧**。

**为什么？** 因为桥接层一次只拿到**一条逻辑消息**（一个 WS 帧 / 一次 gRPC 调用），无法在桥接层重放一个真正的客户端流。源码注释明确写：真正的 client-streaming fan-in / bidi 语义**只在原生 gRPC 同协议直连路径上可用**。只有 server-streaming（`TALK_ONE_ANSWER_MORE`）在桥接层保留了“收集/聚合多条响应”的能力。

> **实践建议**：要演示或测试真正的四种流式语义，直接用 `grpcurl` 打某个 gRPC 端口并配 gRPC 下游（见 §10 示例 D）；经默认链路时，除 server-streaming 外，其余流式方法都表现为一元。

### 8.3 本地处理（终端 S3，收 WS 后本地处理）

S3 收 WS，本地处理走 `ChainHandler.processLocally`。由于上游两个桥接点已把 client-streaming/bidi 压成一元，落到 S3 时按 `method` 返回单条结果；server-streaming（`TALK_ONE_ANSWER_MORE`）则按逗号拆分 `data` 返回多条——但注意 S2 的 WS 往返只承载一条逻辑消息，因此 S3 的多条结果会作为一个 `JanusMessage`（`results` 列表含多项）整体回传，S2 再聚合进单个 `TalkResponse` 回给 S1。

---

## 9. 服务生命周期：启动、运行、停止

前面各章讲的是"消息怎么走"。这一章换一个维度，讲"进程怎么活"——一个节点从启动、稳定运行到优雅退出，整个生命周期里的工程细节：角色如何形成、连接如何复用、如何探活、如何在故障下保持韧性、如何做到高并发低延迟，以及如何干净地下线。

### 9.1 启动：同一份代码如何长成不同角色

回到最初那个问题：三个节点是同一个 jar，凭什么一个成了 WS 入口、一个成了协议转换的中间站、一个成了本地处理的终端？答案是：**代码里没有任何按节点名分叉的逻辑，全部决策都归结为对同一组配置谓词的读取**。这些谓词定义在 [`ServerConfig`](../src/main/java/org/janus/config/ServerConfig.java)：

```java
hasDownstream()     // DOWNSTREAM_PROTOCOL != none
isWsDownstream()    // == ws
isGrpcDownstream()  // == grpc
isNacosDiscovery()  // DOWNSTREAM_DISCOVERY == nacos
isEtcdDiscovery()   // == etcd
registerNacos()     // REGISTER == nacos
registerEtcd()      // == etcd
```

角色是这些谓词在四个装配点上"分别求值"叠加出来的结果，没有一个集中的 `role` 字段：

**装配点一：建立哪些注册中心连接**（`createRegistries()`）。注意这里有**两个独立的槽位**——一个用于注册（`registry`），一个用于发现（`discoveryRegistry`），二者可以指向不同的注册中心：

```java
if (registerNacos())      registry = new NacosRegistry(...);
else if (registerEtcd())  registry = new EtcdRegistry(...);

if (isNacosDiscovery())      discoveryRegistry = new NacosRegistry(...);
else if (isEtcdDiscovery())  discoveryRegistry = new EtcdRegistry(...);
```

于是 S2 这一个节点，`registry` 是 Nacos（注册自己），`discoveryRegistry` 是 etcd（发现 S3）——两套注册发现在同一进程里并存，正是靠这两个槽位。

**装配点二：连不连下游客户端、连哪种**（`start()` 第 4、6 步）：

```java
if (isGrpcDownstream()) { grpcClient = new JanusGrpcClient(); grpcClient.connect(discoveryRegistry); ... }
if (isWsDownstream() && discoveryRegistry != null) { wsClient = new JanusWsClient(...); wsClient.connect(); ... }
```

S1 只装配 `wsClient`，S2 只装配 `grpcClient`，S3 两个都不装配。

**装配点三：gRPC 服务实现是"转发"还是"本地处理"**。这一点最能体现"同一份代码两副面孔"：`JanusServiceImpl` 只有一份，行为却由"下游 stub 是否被注入"决定：

```java
// start() 第 4 步：只有 grpc 下游节点才注入 stub
if (grpcClient.isConnected()) {
    grpcService.setBlockingStub(grpcClient.getBlockingStub());
    grpcService.setAsyncStub(grpcClient.getAsyncStub());
}
// JanusServiceImpl.talk() 里：
if (blockingStub == null) { /* 本地处理，生成问候语 */ }
else { response = blockingStub.talk(request); /* 继续转发 */ }
```

S3 的 stub 为 null → 本地处理（终端）；如果哪天把一个节点配成"gRPC 收、gRPC 转发"，同样这段代码就变成了 gRPC 中继。**代码没变，注入变了，角色就变了。**

**装配点四：注册时上报什么端口和协议**（`registerService()`）。注册中心不同，上报的内容也不同，因为下游用不同协议来连它：

```java
// 公布协议/端口与注册中心解耦，由 JANUS_REGISTER_PROTOCOL 决定
int port = registerAsGrpc() ? GRPC_PORT : WS_PORT;
registry.register(SVC_DISC_NAME, ADVERTISED_HOST, port, REGISTER_PROTOCOL);
```

把四个装配点对三个节点求值，就得到完整的"角色画像"：

| 装配点 | S1（入口） | S2（中间） | S3（终端） |
|--------|-----------|-----------|-----------|
| `registry`（注册槽） | 无（REGISTER=none） | Nacos | etcd |
| `discoveryRegistry`（发现槽） | Nacos | etcd | 无 |
| 下游客户端 | `JanusGrpcClient` | `JanusWsClient` | 无 |
| gRPC stub 注入 | 是（下游是 gRPC，注入 stub） | 否（下游是 WS，经 ChainHandler 走 WS） | 否 → 本地处理 |
| 注册上报 | 不注册 | `grpc` @ GRPC_PORT | `ws` @ WS_PORT |
| `ChainHandler.route()` 走向 | `forwardViaGrpc` | `forwardViaWs` | `processLocally` |

> 补充一个容易忽略的点：**每个节点都同时启动了 WS 服务端和 gRPC 服务端**（`start()` 第 7 步），差别只在"往哪转发 / 注册发现到哪"。所以即便是 S1，它的 gRPC 端口也在监听、S3 的 gRPC 端口也在监听，只是默认链路上没有人从这些方向进来。

运行期的分发同样只读这组谓词——`ChainHandler.route()` 在每次请求到达时重新判断 `isGrpcDownstream() / isWsDownstream()`，没有任何缓存的"角色状态"。这意味着角色是**纯配置驱动、无状态**的：改环境变量重启即换角色，无需改代码、无需重新编译。

### 9.2 运行 · 连接复用

两段链路用两种截然不同、但都做到"连接复用"的机制。

**WS 段（S2→S3）：应用层多路复用连接池**。[`JanusWsClient`](../src/main/java/org/janus/ws/JanusWsClient.java) 维护 `WS_POOL_SIZE`（默认 8）条到下游的长连接。WebSocket 本身没有 HTTP/2 那样的原生流多路复用，于是项目在应用层自己实现：每条连接上可以同时压入大量在途请求，用信封里的 `request_id` 把响应对回请求。

```java
// 发送前先登记 future，再发；回复到达时按 request_id 唤醒
pending.put(correlationId, future);
send(json);
return future.get(timeoutMs, MILLISECONDS);
// onMessage：extractRequestId(message) → pending.remove(corr).complete(message)
```

`pickHealthy()` 用原子计数做轮询，把请求分散到池里已打开的连接上。这套设计的意义在于：**连接数与并发请求数解耦**——8 条连接可以扛住成千上万的并发在途请求，而不是一请求一连接。

**gRPC 段（S1→S2）：HTTP/2 原生多路复用 + 长连接**。[`JanusGrpcClient`](../src/main/java/org/janus/grpc/JanusGrpcClient.java) 建一个 `ManagedChannel`，gRPC 基于 HTTP/2，天然在单条 TCP 连接上多路复用多个流（stream），无需应用层介入。配合 `round_robin` 负载均衡策略，NameResolver 解析出的多个 S2 实例地址被轮询使用；`keepAliveWithoutCalls(true)` 让空闲连接也保持热态，避免每次请求都要重建连接。

| 维度 | WS 段（JanusWsClient） | gRPC 段（JanusGrpcClient） |
|------|----------------------|---------------------------|
| 复用层级 | 应用层，自建多路复用 | 传输层，HTTP/2 原生 |
| 连接数 | 固定池 `WS_POOL_SIZE`=8 | 单 Channel（内部按需建流） |
| 并发关联 | `request_id` ↔ `CompletableFuture` | HTTP/2 stream id（框架管理） |
| 负载均衡 | 池内轮询 + 按发现实例分散 | `round_robin` over NameResolver 地址集 |
| 保活 | 协议 ping（见 §9.3） | HTTP/2 keepalive |

### 9.3 运行 · 探活与健康检查

链路上有**四条相互独立的探活通道**，各管一段。理解它们的周期和失败后果，是排查"为什么流量还在打一个已经挂了的节点"的关键。

| 通道 | 探测方 | 机制 | 周期 / 阈值 | 失败后果 |
|------|--------|------|-------------|----------|
| Nacos 实例健康 | Nacos 客户端 ↔ 服务端 | 客户端自动心跳；`discover` 用 `selectInstances(healthy=true)` 过滤 | 客户端库默认心跳 | 不健康实例不进 S1 的连接池候选 |
| etcd 租约 | S3 ↔ etcd | Lease + `keepAlive` 续租；进程挂→停止续租→TTL 到期删 key | `ETCD_TTL`=10s | key 消失，`EtcdNameResolver` 收到 DELETE，从 gRPC LB 地址集移除 |
| gRPC keepalive | S2 客户端 ↔ S3 服务端 | HTTP/2 PING 帧；服务端 `HealthStatusManager` 置 SERVING | 客户端 10s/超时 1s；服务端 30s/超时 5s | 连接判死，Channel 重连 / 切换实例 |
| WS ping/pong | S1 池 ↔ S2；WS 服务端 ↔ 所有客户端 | 协议级 ping，超时无 pong 即关闭连接 | `WS_CONN_LOST_TIMEOUT_SEC`=20s | 连接关闭 → `onClose` → 失败在途请求 + 从池中剔除 |

> **本次补齐的实现**：WS 转发连接此前只靠 `isOpen()` 判活，而半开连接（对端进程崩溃、网络分区，未发 FIN）在库默认下最长要 60s 才被发现——这期间请求会被写进一个黑洞、直到各自超时。现在 `JanusWsServer` 与 `JanusWsClient.MultiplexClient` 都调用了 `setConnectionLostTimeout(WS_CONN_LOST_TIMEOUT_SEC)`（默认 20s，可配、可设 0 关闭），让 Java-WebSocket 主动发 ping、超时即关闭，把半开连接的探测时间从 60s 缩短到 20s，随后由后台维护线程立即补连。

另外，`NacosNameResolver` 在发现结果**瞬时为空**时不清空 gRPC 负载均衡器地址集，而是保留 `lastGood`——因为一个 peer 刚注册、心跳还没把它标记为健康时会出现短暂空窗，若此时清空会误伤在途和新请求。真正的整体下线由负载均衡器自己的连通性处理兜底。

### 9.4 运行 · 服务韧性

韧性体现在"局部故障不扩散、不放大"。逐条对应到代码：

- **热路径永不阻塞在重连上**（`JanusWsClient`）：`forward` 只挑已打开的连接；发现和(重)连接都在后台单线程 `maintainer` 上每 3 秒做一次（`ensurePool`）。下游故障时请求**快速失败**（抛"No downstream WS connection available"并触发一次异步补池），而不是让大量 handler 线程串行卡在阻塞重连上——避免了雪崩。
- **安全重试语义**（R1）：只有当 `send` 本身抛异常（字节根本没上线，`NotSentException`）才在另一条连接重试；一旦字节已发出，即便之后失败也**绝不重发**，把错误如实抛给调用方。这保护了非幂等请求不被下游重复处理。
- **负载脱落而非拖垮**：JDK<21 回退的平台线程池用 `AbortPolicy`，饱和时直接拒绝并返回 `503 / server busy`（WS）或 `ERR_DECODE`（Binary），**绝不把处理内联到 I/O 线程**（那会拖垮整个传输层）。JDK21+ 用虚拟线程，基本不触发这条。
- **schema 漂移容忍**：两处 `ObjectMapper` 都设了 `FAIL_ON_UNKNOWN_PROPERTIES=false`，下游返回的错误信封或未来新增字段不会让某一跳解析崩溃。
- **协议解码的防御边界**：`BinaryCodec.readCount()` 对所有长度/数量前缀做上限（`MAX_ELEMENTS`）和剩余字节双重校验，恶意帧无法触发 OOM（见 §5.2）。
- **gRPC**：`enableRetry()` + keepalive + `round_robin`，死实例会被负载均衡器跳过。
- **停机不丢流量**：优雅下线的"先注销、后停服"顺序（§9.6）保证退出过程中上游不再把新流量路由过来。

### 9.5 运行 · 并发、高吞吐、低延迟

三个目标是同一套设计的不同侧面：

**虚拟线程扛并发**（[`ExecutorSupport`](../src/main/java/org/janus/common/ExecutorSupport.java)）。生产运行在 JDK 25，WS 与 gRPC 的处理线程池都用 `Executors.newVirtualThreadPerTaskExecutor()`。链路处理天然是"阻塞式"的（同步等一个下游 gRPC/WS 往返），虚拟线程让"海量并发 + 阻塞等待"的组合成本极低——每个在途请求占一个虚拟线程，几十万并发也只是少量内存，且调度延迟极低。老 JDK 回退到有界弹性平台线程池（`core=min(32,max)`，队列 10000，`AbortPolicy`）。

**I/O 线程永不被业务阻塞**。这是低延迟的关键：

```java
// JanusWsServer.onMessage：读线程只做转交，处理丢给 handlerExecutor
handlerExecutor.execute(() -> { String resp = chainHandler.handleJsonRequest(...); conn.send(resp); });
// JanusGrpcServer：NettyServerBuilder.executor(handlerExecutor)
```

WS 的读线程、Netty 的 EventLoop 都不会陷在一次下游往返里，始终能接收/派发新消息。

**多路复用消除队头阻塞**。`JanusWsClient` 的多路复用池此前有过一版"单连接 + 全局锁、一次只能一个在途请求"的实现，会把并发请求串行化。现在每个请求各自持有一个 `CompletableFuture`、按 `request_id` 独立完成，互不阻塞。仓库里的 [`WsForwardingConcurrencyTest`](../src/test/java/org/janus/ws/WsForwardingConcurrencyTest.java) 正是并发打 200 个请求、断言每个都拿到**属于自己**的正确响应（无串扰、无串行停顿）的回归测试。

**gRPC 的吞吐旋钮**（`JanusGrpcServer` / `JanusGrpcClient`）：HTTP/2 流多路复用 + `flowControlWindow`（默认 8MB，高 BDP 链路提吞吐）+ `maxInboundMessageSize`（16MB）+ 可配的 `maxConcurrentCallsPerConnection`。

可调参数一览：

| 环境变量 | 默认 | 作用面 |
|----------|------|--------|
| `JANUS_WS_POOL_SIZE` | 8 | WS 转发连接池大小（分散 TCP 连接/实例） |
| `JANUS_WS_FORWARD_TIMEOUT_MS` | 10000 | 单次 WS 下游往返超时 |
| `JANUS_HANDLER_MAX_THREADS` | 512 | 平台线程回退池上限（JDK21+ 用虚拟线程时忽略） |
| `JANUS_GRPC_MAX_INBOUND_MSG` | 16777216 | gRPC 最大入站消息字节 |
| `JANUS_GRPC_MAX_CONCURRENT_CALLS` | 0（不限） | 单连接最大并发调用 |
| `JANUS_GRPC_FLOW_WINDOW` | 8388608 | gRPC HTTP/2 流控窗口 |

### 9.6 停止：优雅下线

停机的目标只有一个：**退出过程中不给上游制造错误**。要做到这点，顺序至关重要。[`JanusServer.stop()`](../src/main/java/org/janus/JanusServer.java) 由 JVM shutdown hook（`docker stop` 发 SIGTERM 时触发）调用，按下面顺序执行，并用 `AtomicBoolean` 保证只跑一次：

```
0. compareAndSet 去重      hook 与显式调用不会重复执行
1. 先从注册中心注销         registry.deregister() —— etcd DELETE / Nacos deregister
2. 排空窗口 sleep(drain)    SHUTDOWN_DRAIN_MS 默认 2000ms
3. 停入站监听               wsServer.stop(1000)（排空 handler 池）+ grpcServer.stop()
4. 关下游客户端             wsClient.shutdown() / grpcClient.shutdown()
5. 关注册中心连接 + 刷 OTel  registry.close() / OtelSupport.shutdown()（flush spans）
```

**为什么这个顺序是对的（本次修正的核心）**：

> 修正前的实现是"**先停 WS 服务端、最后才注销**"。这留下一个致命窗口——节点已经不再服务，却仍然注册在 Nacos/etcd 里可见，上游的发现机制会继续把新流量路由过来，这些请求必然失败。这不是优雅下线，是"带病下线"。
>
> 修正后：**先注销 → 排空 → 再停服**。注销让上游的解析器（etcd watch 收到 DELETE、Nacos 心跳/查询感知到实例消失）尽快把新流量引开；排空窗口给"上游感知延迟 + 在途请求完成"留出时间，其间节点仍在服务、也仍能使用下游客户端；两者都完成后才关闭监听器。

**几个实现细节**：

- 第 3 步 `grpcServer.stop()` 会先把健康状态置为 `NOT_SERVING`（`healthStatusManager.enterTerminalState()`），再 `shutdown()` 并 `awaitTermination(10s)`，超时才 `shutdownNow()`——gRPC 侧本身就有排空。
- `wsServer.stop(timeout)` 现在会在关闭后 `awaitTermination` 处理线程池（受同一超时约束），让在途 handler 任务尽量跑完再强制结束，而不是直接丢弃。
- 一个诚实的边界：注销只能引开**新的**发现流量；S1 到 S2 已经建立的 WS 长连接不会因为 S2 注销就断开，它们会一直用到被关闭。真正切断这些存量连接的是第 3 步——`wsServer.stop` 关闭连接后，S1 的 `MultiplexClient.onClose` 触发 `failAllPending` 并把该连接踢出池。所以是"注销 + 排空 + 关闭"三步合起来，才实现端到端的平滑下线。
- 排空窗口可通过 `JANUS_SHUTDOWN_DRAIN_MS=0` 关闭（本地快速重启 / 测试场景）。

---

## 10. 动手实践

前置：按 README 启动完整栈（`docker compose -f docker/docker-compose.yml --project-directory . up --build`）。端口映射见 README「服务端口」。

### 示例 A —— Unary 全链路（WS JSON → S1 → S2 → S3）

```bash
echo '{"method":"TALK","mode":"REQUEST","data":"0","meta":"demo","seq":0,"stream_end":true}' \
  | websocat ws://localhost:8080/json
```

预期响应含 `"data":"Hello,Thank you very much"`。换 `data` 为 `1..5` 观察不同语言。

### 示例 B —— Server Streaming（一请求多响应）

```bash
echo '{"method":"TALK_ONE_ANSWER_MORE","mode":"REQUEST","data":"0,1,2","meta":"demo","seq":0,"stream_end":true}' \
  | websocat ws://localhost:8080/json
```

经 WS 入口时 S2 会把多条下游响应合并进一个 `results` 数组返回（见 §8.3）。

### 示例 C —— 手工构造 WS Binary MSG_JANUS 帧

目标：构造 `TALK / REQUEST / data="0" / meta="test"`。按 §5.2 的顺序拼字节：

```
帧头:  48 01 10 00  00 00 00 28          # H, v1, MSG_JANUS, flags0, PAYLOAD_LEN=40
载荷:                                     #                                    字节数
  00                                     # method = TALK(0)                      1
  00                                     # mode   = REQUEST(0)                   1
  00 00 00 00                            # seq    = 0                            4
  01                                     # streamEnd = true                      1
  00 00 00 00                            # status = 0（REQUEST 不用）            4
  00 00 00 01 30                         # data   = "0"      (u32 len=1 + '0')   5
  00 00 00 04 74 65 73 74                # meta   = "test"   (u32 len=4 + 4B)    8
  00 00 00 00                            # traceId = ""      (u32 len=0)         4
  00 00 00 00                            # spanId  = ""                          4
  00 00 00 00                            # errorMsg= ""                          4
  00 00 00 00                            # results_count = 0                     4
```

> PAYLOAD_LEN 校验：1+1+4+1+4 + 5 + 8 + 4 + 4 + 4 + 4 = **40 = 0x28**。用 Postman 的 WebSocket Binary 模式（连 `ws://localhost:8080/binary`）发送。连上后服务端会先回一帧 `BONJOUR`。若字节数与声明的长度不符，会被 `decodeFrame`/`readCount` 拒绝并回 `MSG_ERROR`。

### 示例 D —— 直接打 gRPC，验证真正的四种模型

```bash
grpcurl -plaintext localhost:9093 list                         # 列服务（需 reflection=Y）
grpcurl -plaintext -d '{"data":"0","meta":"t"}' localhost:9093 janus.JanusService/Talk
grpcurl -plaintext -d '{"data":"0,1","meta":"t"}' localhost:9093 janus.JanusService/TalkOneAnswerMore

# 客户端流 / 双向流（交互式，@ 从 stdin 读多条）
grpcurl -plaintext -d @ localhost:9093 janus.JanusService/TalkMoreAnswerOne
{"data":"0","meta":"t","seq":0}
{"data":"1","meta":"t","seq":1,"stream_end":true}
# Ctrl+D 结束
```

> 打 `localhost:9093`（S3 的 gRPC）时，S3 没有下游、也不是 WS 转发节点，故 `JanusServiceImpl` 走 **本地处理**分支，能看到四种模型的原生流式行为。
> 反之，打 `localhost:9091`（S1 的 gRPC）会**进入链路**：S1 配了 gRPC 下游（`blockingStub!=null`），请求被转发到 S2，再经 S2 的 gRPC→WS 桥接降级（client-streaming / bidi 收敛为一元），最终由 S3 本地处理。

### 示例 E —— 观察注册发现的真实 payload

**Nacos**（S2 注册的实例）：

```bash
curl "http://localhost:8848/nacos/v1/ns/instance/list?serviceName=janus-server"
# 返回 hosts[]，每项含 ip / port(9090) / healthy / metadata.protocol=grpc
```

**etcd**（S3 注册的 KV）：

```bash
docker exec etcd etcdctl get janus-server --prefix
# key:   janus-server/ws://janus-server-iii:8080
# value: ws|<leaseId>
```

杀掉 S3 观察 etcd key 在 TTL（10s）内消失：

```bash
docker stop janus-server-iii
sleep 12
docker exec etcd etcdctl get janus-server --prefix   # key 应已被租约到期清除
```

### 示例 F —— 看一条完整 trace

发一条示例 A 的请求后：

- Jaeger UI `http://localhost:16686`，service 选 `janus-server-i`，找到含 5 个 span 的 trace（S1 server/client、S2 server/client、S3 server）。
- Grafana `http://localhost:3000`（admin/admin）用 Loki 查 `{container_name="janus-server-ii"}`，从日志的 `traceId` 跳到 Jaeger。

### 示例 G —— 观察优雅下线的顺序

对中间节点 S2 发一次 `docker stop`，跟读它的下线日志，验证"先注销、后停服"的顺序（见 §9.6）：

```bash
docker stop janus-server-ii
docker logs janus-server-ii --tail 20
```

预期日志依次出现：`Shutting down Janus Server (graceful)` → `Deregistered [janus-server] from Nacos` → `Draining for 2000 ms before closing listeners` → 停 WS/gRPC 监听 → `Janus Server stopped`。若把 `JANUS_SHUTDOWN_DRAIN_MS` 设为 `0` 重启该容器，则不再出现 Draining 一行、下线更快（适合本地快速迭代）。

停机期间从 S1 持续发请求（示例 A 循环），可观察到：注销后新流量被引开、排空窗口内在途请求仍能正常返回，不会出现"打到已停节点"的错误。

---

## 11. 排错对照表

| 症状 | 可能原因 | 排查/定位 |
|------|----------|-----------|
| WS 连不上，握手被关（POLICY_VALIDATION / unauthorized） | 开了 `JANUS_AUTH_TOKEN` 但客户端没带 `authToken` 头 | `JanusWsServer.onOpen` 的鉴权分支；用 `wscat -H "authToken: <token>"` |
| S1 gRPC 报无可用地址（"no servers online"） | S2 未注册到 Nacos，或 `nacos://` target 拼错 | `NacosNameResolver` 日志；查 Nacos 实例列表（示例 E） |
| S2 报 "No downstream WS instances discovered" | S3 未注册到 etcd，或 etcd 未就绪 | `EtcdRegistry.discover` 返回空；`etcdctl get janus-server --prefix` |
| etcd 里 host 解析成 null | key 前缀剥离逻辑不一致 | 必须用 `getUriFromDir`（先把 `://` 换 `~` 再切）；见 §4.4 |
| 每个 gRPC 调用都失败/中止 | 拦截器用 `TRACING_KEYS.size()`(7) 遍历 `CONTEXT_KEYS`(3) 越界 | 已修复为按 `CONTEXT_KEYS.size()` 遍历；见 §7.3 |
| trace 断成两条 | 入站上下文没被 server span 回写 carrier | `TracingHelper.startServerSpan` 的 `inject` 回写；WS 用 `mergeTraceContext` 重建 traceparent |
| 桥接（WS→gRPC 或 gRPC→WS）的 client-streaming/bidi 表现像 unary | 桥接层刻意降级 | 设计如此，见 §8.2；要原生流式请直连终端 gRPC（示例 D） |
| 下游 WS 超时但请求似乎被处理了 | 已发出的请求不重发（避免重复处理） | `JanusWsClient.forward` 的安全重试语义（R1） |
| WS 返回 `{"status":503,"error_msg":"server busy"}` | 平台线程回退池饱和（仅 JDK<21） | 提高 `JANUS_HANDLER_MAX_THREADS` 或用 JDK21+（虚拟线程） |
| Binary 帧被拒（ERR_DECODE / bad magic / truncated） | 帧头或长度前缀不对 | 核对 MAGIC=0x48、VERSION=0x01、PAYLOAD_LEN、`readCount` 边界；见 §5.2 |
| MSG_JANUS 解析字段错位 | 载荷字段顺序与 `encodeJanus` 不一致 | 严格按 method,mode,seq,streamEnd,status,data,meta,traceId,spanId,errorMsg,requestId,results |
| 二进制转发回复对不上/超时 | 响应帧未回写 `request_id`，或帧内 `request_id` 与请求不一致 | `handleBinaryJanus` 里 `response.withRequestId(frame.requestId())`；`JanusWsClient.onMessage(ByteBuffer)` 按 `decodeJanus().requestId()` 关联 |
| 停机瞬间上游偶发请求失败 | 停机顺序未先注销、或排空窗口过短 | 已改为"先注销→排空→停服"（§9.6）；调大 `JANUS_SHUTDOWN_DRAIN_MS` |
| 对端崩溃后 WS 请求卡到超时才失败 | 半开连接未被及时探测 | 已加 `setConnectionLostTimeout`（`JANUS_WS_CONN_LOST_TIMEOUT_SEC`，默认 20s）；见 §9.3 |

---

## 12. 负载均衡、限流、熔断：两种协议的原理详解

链路里有两段截然不同的传输——S1→S2 的 **gRPC** 与 S2→S3 的 **WS Binary**。它们在“把请求分散到多个实例（负载均衡）、在压力下保护自己（限流/背压）、在下游故障时快速失败并摘除坏节点（熔断）”这三件事上，用的是**两套完全不同的机制**。本节把两条路径逐一拆开讲原理，并明确指出**哪些是已实现的、哪些只是接近而非教科书式的完整实现**（避免过度宣称）。

> 一句话总览：
> - **gRPC 段**：负载均衡交给 gRPC 框架的 `round_robin` LB + 自定义 NameResolver；限流靠 HTTP/2 流控窗口 + 单连接并发上限；“熔断”靠发现层的健康摘除 + 连接不可用时的 503 快速失败。
> - **WS 段**：负载均衡靠自建连接池按实例分散 + 连接级轮询；限流靠有界处理线程池（饱和即 503）+ 连接池大小 + 单请求超时；“熔断”靠无连接时立即快速失败 + ping/pong 探活摘除半开连接 + 已发送不重试。

### 12.1 负载均衡

**gRPC 段（S1→S2）——框架原生 LB + 自定义 NameResolver**

原理分三层：

1. **NameResolver 解析出地址集**。`JanusGrpcClient` 用 `nacos://janus-server` 作为 target，注册 [`NacosNameResolver`](../src/main/java/org/janus/discovery/NacosNameResolver.java)。它 `subscribe` Nacos，把 `selectInstances(serviceName, true)`（只取健康实例）映射成一组 `EquivalentAddressGroup` 推给 gRPC（`listener.onAddresses(...)`）。成员变化被推送，免重启。
2. **LB 策略在地址集上轮询**。builder 设 `defaultLoadBalancingPolicy("round_robin")`（`Constants.LB_ROUND_ROBIN`）。gRPC 为每个地址建一个 subchannel，`round_robin` 把连续 RPC 依次分发到各 subchannel。因为 gRPC 基于 HTTP/2，**多个 RPC 在每条 subchannel 上还能并发多路复用**（见 §12.2）。
3. **粒度是“每次 RPC”**。gRPC 的 LB 作用在 RPC 级别，比“每连接”更细：一条长连接上的多个并发调用天然分摊到多个 subchannel。

> 健壮性细节：`NacosNameResolver` 在解析结果**瞬时为空**（peer 刚注册、心跳未标记健康）时**不清空** LB 地址集，而是保留 `lastGood`，避免一次抖动打断在途/新请求。

**WS 段（S2→S3）——自建连接池 + 两级分散**

WebSocket 没有 HTTP/2 那种原生 LB，[`JanusWsClient`](../src/main/java/org/janus/ws/JanusWsClient.java) 在应用层自己实现：

1. **建池时按实例分散**。`ensurePool()` 调 `registry.discover(service)`（etcd）拿到实例列表，补足 `WS_POOL_SIZE`（默认 8）条连接时用 `instances.get(i % instances.size())` 轮流连不同实例——连接被摊到所有已发现的下游实例上。
2. **发请求时按连接轮询**。`pickHealthy()` 用一个原子计数 `roundRobin.getAndIncrement()` 对池内连接取模轮询，只挑 `isOpen()` 的连接，从而把请求分散到多条连接（进而分散到多个实例）。
3. **连接数与并发数解耦**。每条连接靠 `request_id` 多路复用大量在途请求（WS Binary 的 `request_id` 就在 MSG_JANUS 帧里），所以 8 条连接能扛住成千上万并发在途请求，而不是一请求一连接。

> 对比：gRPC 的 LB 粒度是“每 RPC”，由框架管理；WS 的 LB 粒度是“每次 forward 选一条连接”，由本项目在 `pickHealthy` 里手写。两者都用 round-robin，但一个在传输层、一个在应用层。

### 12.2 限流 / 背压

> 诚实说明：本项目**没有**内置“每秒 N 次”的令牌桶 / 漏桶 QPS 限流器。这里的“限流”是**资源上界 + 背压 + 负载丢弃**这一组机制——在过载时保护节点不被拖垮，而不是按固定速率整形流量。若需要真正的 QPS 配额，应在入口加限流器（如 Resilience4j RateLimiter / 网关层）。

**gRPC 段的资源上界（[`JanusGrpcServer`](../src/main/java/org/janus/grpc/JanusGrpcServer.java) / `JanusGrpcClient`）**

- **单连接并发调用上限**：`maxConcurrentCallsPerConnection(JANUS_GRPC_MAX_CONCURRENT_CALLS)`（>0 时生效，默认 0=不限）。这是 HTTP/2 层面对“一条连接上同时在飞的流数量”的硬上限，超出的流被排队/拒绝——是最接近“并发限流”的开关。
- **HTTP/2 流控窗口**：`flowControlWindow(JANUS_GRPC_FLOW_WINDOW，默认 8MiB)`。这是**背压**的核心：接收方通过窗口告诉发送方“还能再收多少字节”，窗口耗尽时发送方必须暂停，天然阻止快生产者压垮慢消费者。
- **最大入站消息**：`maxInboundMessageSize(JANUS_GRPC_MAX_INBOUND_MSG，默认 16MiB)`，防止超大消息撑爆内存。
- **处理线程**：RPC 处理跑在 `ExecutorSupport.newHandlerExecutor` 上（JDK21+ 虚拟线程，几乎无并发上限；旧 JDK 回退为有界池，见下）。

**WS 段的资源上界（[`JanusWsServer`](../src/main/java/org/janus/ws/JanusWsServer.java) / `JanusWsClient`）**

- **有界处理线程池 + 饱和丢弃（背压的关键）**：入站消息不在 I/O 线程处理，而是提交给 `handlerExecutor`。在 JDK<21 回退路径上它是一个**有界** `ThreadPoolExecutor`（`core=min(32,max)`、`max=JANUS_HANDLER_MAX_THREADS` 默认 512、队列 10000、`AbortPolicy`）。队列+线程都满时 `execute` 抛 `RejectedExecutionException`，`onMessage` 捕获后**立即回一个 `503 server busy`**（JSON 文本或 `MSG_ERROR` 二进制帧）——即**负载丢弃**，且绝不把工作内联到 I/O 线程（那会拖垮传输）。JDK21+ 用虚拟线程时这条基本不触发。
- **转发连接池大小**：`WS_POOL_SIZE` 限定 S2→S3 的并发 TCP 连接数（连接复用而非无限扩张）。
- **单请求超时（在途上限 + 快速回收）**：`WS_FORWARD_TIMEOUT_MS`（默认 10s）。`future.get(timeout)` 超时即报错回收 `pending` 槽位，避免慢下游把在途请求无限堆积。

> 小结：gRPC 段的背压是**协议内建**（HTTP/2 流控窗口）；WS 段的背压是**应用层构造**（有界池 + 饱和 503 + 超时）。两者都遵守同一原则：**过载时快速拒绝，而不是排队到 OOM 或阻塞 I/O 线程**。

### 12.3 熔断 / 快速失败与坏节点摘除

> 诚实说明：本项目实现的是**“快速失败 + 基于发现的坏节点摘除 + 探活”**，而**不是**带独立 `CLOSED→OPEN→HALF_OPEN` 状态机、按错误率跳闸的经典熔断器（如 Resilience4j CircuitBreaker）。下面讲清楚“熔断语义”在两段各由什么承担。

**共同的第一道闸：转发节点不静默回退（[`ChainHandler.route()`](../src/main/java/org/janus/handler/ChainHandler.java)）**

被配置为转发角色的节点（S1/S2），当下游客户端不可用时**不会**回退到本地处理（那会用伪造结果掩盖下游故障），而是立即返回 `status=503` 的 `ERROR` 信封，让故障沿链路上抛：

```java
if (isGrpcDownstream()) {
    if (grpcClient != null && grpcClient.isConnected()) return forwardViaGrpc(...);
    return JanusMessage.error(method, 503, "downstream gRPC unavailable");   // 快速失败
}
if (isWsDownstream()) {
    if (wsClient != null && wsClient.isConnected()) return forwardViaWs(...);
    return JanusMessage.error(method, 503, "downstream WS unavailable");     // 快速失败
}
```

**gRPC 段的“熔断”构成**

- **坏节点摘除（发现层）**：Nacos 只回健康实例（`selectInstances(...,true)`），宕掉的实例被心跳判死后从地址集移除；`round_robin` 自然不再选它。等价于对单个坏后端“断流”。
- **连接级健康**：`keepAliveWithoutCalls(true)` + keepAlive ping 探活；`enableRetry()` 允许对可安全重试的调用重试。
- **快速失败**：`isConnected()` 检查 channel 未关闭；channel 处于 `TRANSIENT_FAILURE` 时 RPC 快速失败而非无限挂起。
- **未实现**：没有“错误率超阈值就整体跳闸一段时间”的 OPEN 状态；恢复靠发现层重新纳入健康实例。

**WS 段的“熔断”构成**

- **无连接即刻快速失败**：`forward/forwardBinary` 若 `pickHealthy()` 挑不到打开的连接，**立即**抛 `No downstream WS connection available` 并触发后台异步补池——**绝不**在热路径上阻塞重连（R4）。这就是 WS 段的“断路”效果：下游整体不可用时，请求毫秒级失败而非线程堆积。
- **半开连接探测摘除**：`setConnectionLostTimeout(JANUS_WS_CONN_LOST_TIMEOUT_SEC，默认 20s)` 让库周期性 ping，超时无 pong 即关闭连接 → `onClose` 触发 `failAllPending` + 维护线程把它移出池 → 快速重连。避免请求写进“黑洞”半开连接。
- **已发送不重试（防重复副作用）**：只有 `send` 本身失败（字节没上线，`NotSentException`）才在另一条连接重试；一旦字节已发出，超时/中途失败一律上抛不重发（R1），避免下游对非幂等请求重复处理。
- **后台自愈**：维护线程每 3 秒 `ensurePool()` 补齐健康连接，等价于“半开探测→恢复”。

**两段对照速查**

| 维度 | gRPC 段（S1→S2） | WS 段（S2→S3） |
|------|------------------|----------------|
| 负载均衡粒度 | 每 RPC（`round_robin` LB） | 每 forward 选连接（`pickHealthy` 轮询） |
| 地址来源 | `NacosNameResolver`（push/subscribe） | `EtcdRegistry.discover`（池维护线程拉取） |
| 多路复用 | HTTP/2 原生（每连接多流） | 应用层，`request_id` 关联（含二进制帧字段） |
| 背压/限流 | HTTP/2 流控窗口 + 单连接并发上限 + 最大消息 | 有界处理池饱和回 503 + 池大小 + 转发超时 |
| 坏节点摘除 | 发现层健康过滤 + LB 不再选 | ping/pong 超时关连接 + 移出池 |
| 下游全不可用 | `route()` 回 503（不回退本地） | `route()` 回 503 + 无连接即刻快速失败 |
| 重试安全 | `enableRetry()`（幂等安全范围） | 仅“未发送”可重试，已发送不重发 |
| 是否经典熔断器 | 否（无 OPEN 跳闸状态机） | 否（无 OPEN 跳闸状态机） |

> 想要真正的熔断器 / QPS 限流，可在此基础上接入 Resilience4j（CircuitBreaker + RateLimiter）或在入口网关做，二者与本项目的快速失败/背压机制是叠加关系，不冲突。

---

## 附：组件速记

- **JanusServer**：装配工，九步把零件拼成一个完整节点。
- **ServerConfig**：角色开关，三个环境变量决定一个节点是入口、中间还是终端。
- **JanusMessage**：同一条逻辑消息，套三件外衣（JSON / Binary / gRPC）。
- **ChainHandler**：翻译官兼调度员，解析入站、决定转发还是本地处理、完成协议转换。
- **BinaryCodec**：字节工匠，`encodeJanus` 与 `decodeJanus` 严格镜像对称。
- **JanusWsClient**：多路复用连接池，一条连接承载大量在途请求，靠 `request_id` 认领响应。
- **JanusGrpcClient**：按发现协议挑选 NameResolver 的拨号器。
- **NacosRegistry**：结构化实例 + metadata + 客户端自动心跳。
- **EtcdRegistry / EtcdNameResolver**：KV 编码（key 带 `<scheme>://`，scheme 反映公布协议）+ 租约健康检查 + watch 增量维护。
- **TracingHelper**：让 server span 回写 carrier，一条 trace 贯穿三种协议。
