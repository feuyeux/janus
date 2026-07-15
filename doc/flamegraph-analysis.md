# 用火焰图分析 Java 代码性能



Java 服务的性能分析通常会遇到以下场景：

- 压测一跑，TPS 上不去，但不知道该看哪里
- 火焰图已生成，但图中的热点和调用路径难以解读
- 图里一堆 `Unsafe.park`、`SocketDispatcher.read0`、`ObjectMapper.readValue`，不知道哪个该优化
- 改完代码以后，感觉“好像快了一点”，但拿不出证据

本文围绕这些场景展开，说明从压测、采集到热点定位和优化排序的完整过程：

1. 启动服务
2. 压测服务
3. 采集 CPU 和内存火焰图
4. 分析热点
5. 把热点定位回代码
6. 给出有优先级的优化建议

本文基于本项目一次真实的 `profile` 结果展开。相关文件如下：

- CPU 火焰图：[`arthas-output/20260715-160848-pid-13947/cpu-30s.html`](../arthas-output/20260715-160848-pid-13947/cpu-30s.html)
- 内存分配火焰图：[`arthas-output/20260715-160848-pid-13947/memory-alloc-30s.html`](../arthas-output/20260715-160848-pid-13947/memory-alloc-30s.html)
- Profiling 脚本说明：[`doc/arthas-profiler.md`](arthas-profiler.md)

---

## 1. 复现：启动服务、压测与生成火焰图

性能分析通常从火焰图解读开始，但图的采集条件直接影响结论。本文首先说明完整复现流程。

### 1.1 本文使用的脚本

本仓库提供了以下完整脚本流程：

1. 启动单进程 Janus
2. 发送 WebSocket JSON 请求制造负载
3. 用 Arthas 生成 CPU 和内存火焰图

对应脚本如下。

启动服务：

- Unix / macOS: [`scripts/janus-local-start.sh`](../scripts/janus-local-start.sh)
- Windows PowerShell: [`scripts/janus-local-start.ps1`](../scripts/janus-local-start.ps1)

请求服务：

- Unix / macOS: [`scripts/janus-request.sh`](../scripts/janus-request.sh)
- Windows PowerShell: [`scripts/janus-request.ps1`](../scripts/janus-request.ps1)
- 负载程序实现：[`scripts/JanusWsLoad.java`](../scripts/JanusWsLoad.java)

生成火焰图：

- Unix / macOS: [`scripts/arthas-flame.sh`](../scripts/arthas-flame.sh)
- Windows PowerShell: [`scripts/arthas-flame.ps1`](../scripts/arthas-flame.ps1)

一键串起全流程：

- Unix / macOS: [`scripts/janus-profile-flow.sh`](../scripts/janus-profile-flow.sh)
- Windows PowerShell: [`scripts/janus-profile-flow.ps1`](../scripts/janus-profile-flow.ps1)

更完整说明见：[doc/arthas-profiler.md](arthas-profiler.md)

### 1.2 一条命令执行完整流程

Unix / macOS:

```bash
chmod +x scripts/*.sh
./scripts/janus-profile-flow.sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\janus-profile-flow.ps1
```

该命令自动完成三项操作：

1. 启动一个本地单节点 Janus
2. 对 `ws://127.0.0.1:8080/json` 持续打请求
3. 生成 `cpu` 和 `alloc` 两张火焰图

输出目录默认在：

```text
arthas-output/<timestamp>-pid-<pid>/
```

例如：

```text
arthas-output/20260715-160848-pid-13947/
```

### 1.3 分步执行

#### 第一步：启动服务

Unix / macOS:

```bash
./scripts/janus-local-start.sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\janus-local-start.ps1
```

这个脚本做的事情是：

- 如果 `target/janus.jar` 不存在，自动执行 `mvn -DskipTests package`
- 以单进程、本地处理模式启动 Janus
- 默认监听：
  - WS: `8080`
  - gRPC: `9090`
  - Metrics: `9100`

启动后的 PID 和日志写到：

- `.run/janus-local.pid`
- `.run/janus-local.log`

#### 第二步：压测服务

Unix / macOS:

```bash
./scripts/janus-request.sh
./scripts/janus-request.sh --duration-seconds 90 --parallelism 8
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\janus-request.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\janus-request.ps1 -DurationSeconds 90 -Parallelism 8
```

这个脚本背后调用的是 [`scripts/JanusWsLoad.java`](../scripts/JanusWsLoad.java)，它直接使用 JDK 自带的 `java.net.http.WebSocket` 客户端：

- 不依赖 `websocat`
- 不依赖 `grpcurl`
- 默认请求 `ws://127.0.0.1:8080/json`

默认参数含义：

- `--duration-seconds 70`
- `--parallelism 4`
- `--pause-millis 0`

也就是说：

- 持续请求 70 秒
- 并发 4 个 worker
- 中间不休眠

#### 第三步：生成火焰图

Unix / macOS:

```bash
./scripts/arthas-flame.sh
./scripts/arthas-flame.sh --cpu-duration 45 --mem-duration 20
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\arthas-flame.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\arthas-flame.ps1 -CpuDuration 45 -MemDuration 20
```

这一步默认会生成两张图：

- `cpu-<N>s.html`
- `memory-alloc-<N>s.html`

首次执行时，脚本会自动下载 `arthas-boot.jar` 到：

```text
.tools/arthas/
```

### 1.4 建议的阅读顺序

首次接触火焰图时，可按以下顺序熟悉产物与热点：

1. 先跑一遍一键脚本，看到产物目录
2. 打开 `cpu-30s.html`
3. 再打开 `memory-alloc-30s.html`
4. 对照本文逐段读
5. 最后再自己独立总结一次热点

第一次分析的重点是识别主要路径，无需在单次阅读中解释全部栈帧。

---

## 2. 火焰图分析中的三个常见误区

火焰图分析中常出现以下直觉性判断：

- 图里最宽的函数，就是“最耗时的业务代码”
- 看到某个函数名字很多次，就是“它一定有问题”
- CPU 图和内存图长得都像火焰图，所以看法应该一样

这些判断缺少上下文。分析时应以以下原则为准：

1. **火焰图的宽度代表“样本占比”，不是源码行数，也不是调用次数。**
2. **CPU 火焰图回答的是：CPU 时间花在哪里。**
3. **alloc 火焰图回答的是：对象分配发生在哪里。**

这两个问题经常相关，但不是一回事。

举个简单例子：

- 某段代码创建了很多临时对象，但计算量不大。它可能在内存图里非常宽，在 CPU 图里却一般。
- 某段代码主要等待网络 I/O。它可能在 CPU 图中表现为 `park`、`read`、`select`，而非业务方法。

因此，单张火焰图不足以支撑性能结论。

---

## 3. 火焰图的阅读方法

### 3.1 横轴和纵轴分别是什么意思

火焰图里：

- 横轴：样本占比，越宽代表占比越高
- 纵轴：调用栈深度，越往上说明调用越深

**横轴不是时间线**。左侧不表示先发生，右侧也不表示后发生；火焰图不同于 trace timeline。

### 3.2 一个方块代表什么

一个方块通常代表一个栈帧，也就是一个方法或一个运行时函数。

本次 CPU 图中可见以下常见栈帧：

- `org/janus/handler/ChainHandler.handleJsonRequest`
- `com/fasterxml/jackson/databind/ObjectMapper.readValue`
- `org/java_websocket/server/WebSocketServer.run`
- `java/util/concurrent/LinkedBlockingQueue.take`
- `jdk/internal/misc/Unsafe.park`

这些名字有三类：

- 业务代码
- 三方库代码
- JVM / JDK 运行时代码

分析范围应覆盖业务代码、协议栈、线程模型和对象分配模式，瓶颈常由后几者引入。

### 3.3 看火焰图时最常用的三个动作

实际工作里，最常用的是这三个动作：

1. 找最宽的块
2. 看这块下面的调用路径
3. 判断这条路径是在“算”“分配”“等”“拷贝”还是“锁竞争”

分析通常从占比最大的热点开始，再逐步展开调用路径。

---

## 4. CPU 火焰图和内存火焰图分别回答什么问题

这次我们有两张图：

- `cpu-30s.html`
- `memory-alloc-30s.html`

它们的职责不同。

### 4.1 CPU 火焰图回答什么问题

CPU 图主要回答：

- CPU 真正忙在哪里
- 是业务计算重，还是线程在等 I/O
- 是锁竞争，还是序列化/反序列化重

大量样本位于以下栈帧时：

- `Unsafe.park`
- `Parker::park`
- `LinkedBlockingQueue.take`
- `Selector.select`
- `SocketDispatcher.read0`

通常表明：**线程的大部分时间未用于执行业务逻辑，而是处于等待状态。**

### 4.2 alloc 火焰图回答什么问题

alloc 图主要回答：

- 大量临时对象是谁分配出来的
- 是 `byte[]` 多，还是 `char[]` 多，还是 `String` 多
- 是 JSON 解析、日志拼接、Map/List 构造，还是线程对象本身在分配

以下栈帧占比较高时：

- `char[]`
- `byte[]`
- `String`
- `ObjectMapper.readValue`
- `ObjectMapper.writeValueAsString`

通常表示：**对象分配压力较大，并可能进一步推高 GC 压力。**

### 4.3 实战时为什么一定要两张图一起看

因为同一个热点，在两张图里表达的意义不同。

比如这次 profile 里：

- CPU 图未显示业务方法耗尽 CPU
- 但 alloc 图非常清楚地表明 JSON 编解码在疯狂分配对象

由此可见：

- 当前问题不是“算不动”
- 而是“每次请求都在生成太多临时对象”

该案例将笼统的“慢”拆分为可验证的具体原因。

---

## 5. 基于真实案例看 CPU 火焰图

先说结论：

**这次 CPU 图最重要的信息，不是某个业务方法特别耗 CPU，而是线程大部分时间在等待 I/O 或等待任务。**

从图里能提炼出几类大头：

- `org/java_websocket/server/WebSocketServer.run`
- `sun/nio/ch/SelectorImpl.select`
- `sun/nio/ch/SocketDispatcher.read0`
- `sun/nio/ch/SocketDispatcher.write0`
- `java/util/concurrent/LinkedBlockingQueue.take`
- `jdk/internal/misc/Unsafe.park`

### 5.1 先看 `park`

CPU 图里很宽的一条路径是：

```text
LinkedBlockingQueue.take
 -> AbstractQueuedSynchronizer$ConditionObject.await
 -> LockSupport.park
 -> Unsafe.park
```

这类栈的意思通常不是“这里有 bug”，而是：

- 有工作线程在等队列里的任务
- 当前线程并没有消耗很多 CPU

这并不构成“`LinkedBlockingQueue` 性能差”的证据。更准确的结论是：

- 当前线程模型里有不少线程处于空闲等待
- 系统不是 CPU 打满型

火焰图中的“宽度”表示样本占比，不等同于异常或缺陷。

### 5.2 再看 `select/read/write`

另一类很宽的路径是：

```text
WebSocketServer.run
 -> Selector.select
 -> KQueue.poll
```

以及：

```text
SocketChannelImpl.read
 -> SocketDispatcher.read0
```

还有：

```text
SocketChannelImpl.write
 -> SocketDispatcher.write0
```

这些路径告诉我们：

- 服务确实在处理网络读写
- 但 CPU 并没有主要耗在“复杂业务逻辑”上
- 大头是网络事件循环和 socket 读写

这说明当前场景更像一个典型网关/协议转换服务，而不是计算型服务。

### 5.3 业务代码在 CPU 图里有没有热点

有，但不大。

比如：

- `org/janus/handler/ChainHandler.handleJsonRequest`
- `org/java_websocket/WebSocketImpl.send`

它们在 CPU 图里出现了，但占比远不如等待和 I/O。

仅依据 CPU 图，可得出以下判断：

> 当前服务并非由业务逻辑耗尽 CPU，优化重点不在算法或局部微优化。

---

## 6. 基于真实案例看内存火焰图

内存分配图比 CPU 图更直接地呈现了可优化的路径。结论如下：

**当前热点是 WS JSON 路径上的 Jackson 编解码，主要体现在大量 `char[]`、`byte[]`、`String` 分配。**

内存图里最关键的路径有两条。

### 6.1 请求反序列化热点

一条非常宽的路径是：

```text
ChainHandler.handleJsonRequest
 -> ObjectMapper.readValue
 -> JsonFactory.createParser
 -> IOContext.allocTokenBuffer
 -> BufferRecycler.allocCharBuffer
 -> char[]
```

对应代码在 [ChainHandler.java](/Users/han/coding/janus/src/main/java/org/janus/handler/ChainHandler.java:51)：

```java
request = objectMapper.readValue(jsonRequest, JanusMessage.class);
```

该路径表明：

- 每次收到一条 WS JSON 文本消息
- Jackson 都要创建解析器
- 申请 token buffer / char buffer
- 再把字符串解析成 `JanusMessage`

如果请求量很大，这条路径就会持续制造大量短命对象。

### 6.2 响应序列化热点

另一条很宽的路径是：

```text
ObjectMapper.writeValueAsString
 -> JsonFactory.createGenerator
 -> WriterBasedJsonGenerator.<init>
 -> IOContext.allocConcatBuffer
 -> BufferRecycler.allocCharBuffer
 -> char[]
```

对应代码在 [ChainHandler.java](/Users/han/coding/janus/src/main/java/org/janus/handler/ChainHandler.java:90)：

```java
return objectMapper.writeValueAsString(response);
```

该路径表明：

- 请求解析之外，响应 JSON 字符串的构造同样产生大量对象分配

换句话说，每个请求至少要经历一次“读 JSON”加一次“写 JSON”。

### 6.3 `char[]` 和 `byte[]` 为什么这么多

因为 JSON 文本协议本质上就依赖字符与字节之间来回转换。

这次图里还能看到这些路径：

- `String.getBytes`
- `Charsetfunctions.utf8Bytes`
- `Charsetfunctions.stringUtf8`
- `CharBuffer.allocate`
- `ByteBuffer.allocate`

这说明除了 Jackson 之外，WebSocket 文本帧本身也有额外成本：

- 收到消息时，要把字节解码成字符串
- 发送消息时，要把字符串编码成字节

因此，该案例的瓶颈并非单纯的“Jackson 慢”，而是：

> **JSON over WebSocket 文本帧整条链路都在放大字符串与缓冲区分配成本。**

---

## 7. 热点与源码的映射

热点已识别为：

- `ObjectMapper.readValue`
- `ObjectMapper.writeValueAsString`
- `String/char[]/byte[]`

随后需要回到代码确认这些热点所在的业务路径，再制定改动方案。

### 7.1 找入口

从 [JanusWsServer.java](/Users/han/coding/janus/src/main/java/org/janus/ws/JanusWsServer.java:140) 看，文本消息入口是：

```java
handlerExecutor.execute(() -> {
    String response = chainHandler.handleJsonRequest(message, traceContext);
    conn.send(response);
});
```

该代码给出了完整处理路径：

1. 收到 WebSocket 文本消息 `message`
2. 交给 `handleJsonRequest`
3. 得到响应字符串
4. 再通过 `conn.send(response)` 发出去

由此可以确认：

- 入站有一次 JSON 解析
- 出站有一次 JSON 序列化
- WebSocket 发送前后还会有字符串/字节转换

### 7.2 看处理逻辑

再看 [ChainHandler.java](/Users/han/coding/janus/src/main/java/org/janus/handler/ChainHandler.java:51) 到 [ChainHandler.java](/Users/han/coding/janus/src/main/java/org/janus/handler/ChainHandler.java:90)：

```java
request = objectMapper.readValue(jsonRequest, JanusMessage.class);
...
JanusMessage response = route(request, ctx, span);
...
return objectMapper.writeValueAsString(response);
```

该方法是火焰图热点对应的核心处理位置。所有 `/json` 请求都会经过此处。

### 7.3 再看对象模型

继续看 [JanusMessage.java](/Users/han/coding/janus/src/main/java/org/janus/model/JanusMessage.java:22)。

`JanusMessage` 是一个 record，字段很多：

- `method`
- `mode`
- `data`
- `meta`
- `status`
- `results`
- `error_msg`
- `trace_id`
- `span_id`
- `seq`
- `stream_end`
- `request_id`

而 `results` 里面又是：

- `List<JanusResult>`
- `JanusResult` 里又有 `Map<String, String> kv`

这意味着：

- 解析时字段不少
- 序列化时对象层次也不少
- 如果响应量大，`List`、`Map`、`String` 都会增加分配成本

火焰图并不直接指出具体缺陷，而是呈现如下结构性事实：

> 这个协议模型配上 JSON 文本传输，在高频请求下天然偏重。

---

## 8. 优化项的优先级

火焰图通常会暴露多项可优化点，但分析结果需要形成明确的**优先级**。

一个简单实用的排序方式是：

1. 结构性问题优先于局部问题
2. 高频路径优先于低频路径
3. 根因优先于结果
4. 能明显缩小热点的项优先

应用到这次案例里：

- “JSON 文本链路分配重”是结构性问题
- “每条消息创建虚拟线程”是线程模型问题
- “UUID.randomUUID()` 会分配对象”是局部问题

前两类问题应优先处理。性能优化以采样证据和优先级排序为依据。

---

## 9. 这次案例的具体优化建议

以下建议按预期收益排序。

### 9.1 优先级最高：高频路径尽量走 Binary，而不是 JSON

这是收益最大的方向。

原因已经从火焰图里看得很清楚：

- JSON 路径要做一次 `readValue`
- 再做一次 `writeValueAsString`
- 还要做 UTF-8 编码/解码

而本项目本身就已经支持 `/binary`，见 [JanusWsServer.java](/Users/han/coding/janus/src/main/java/org/janus/ws/JanusWsServer.java:178) 里的二进制处理路径。

对于高吞吐、低分配和低 GC 的目标：

- 内部链路优先 binary
- 压测也优先 binary
- JSON 更适合作为调试友好、人工可读的入口协议

这属于协议层选择，而非局部优化技巧。

### 9.2 第二优先级：优化 JSON 热路径，而不是全局乱改

业务必须保留 `/json` 时，优化范围聚焦于 [ChainHandler.java](/Users/han/coding/janus/src/main/java/org/janus/handler/ChainHandler.java) 的 `handleJsonRequest`。

可选方向如下：

1. 用 Jackson streaming API 替代通用对象绑定
   当前是：
   - `readValue(jsonRequest, JanusMessage.class)`
   - `writeValueAsString(response)`

   这很方便，但对象绑定层比较重。
   如果热路径只关心少数字段，可以直接用 streaming parser / generator。

2. 对简单 unary 场景做 fast path
   如果最常见请求永远是：
   - `method=TALK`
   - `mode=REQUEST`
   - `data/meta/seq/stream_end`

   那么可以专门为这个场景做更轻的解析和输出，而不是永远走完整通用模型。

3. 尽量减少响应字段和层级
   现在响应里有：
   - `results`
   - `results[].kv`
   - 多个字符串字段

   字段越多，对象越多，序列化分配越多。

### 9.3 第三优先级：为“本地处理模式”和“转发模式”使用不同线程策略

这次分配图里还能看到虚拟线程相关分配：

- `ThreadPerTaskExecutor`
- `VirtualThread`
- `Continuation`

对应代码是 [JanusWsServer.java](/Users/han/coding/janus/src/main/java/org/janus/ws/JanusWsServer.java:140) 与 [ExecutorSupport.java](/Users/han/coding/janus/src/main/java/org/janus/common/ExecutorSupport.java:122)。

当前模式是：

- 每条消息都丢给 `handlerExecutor`
- 在 JDK 25 下，这个 executor 默认是 virtual-thread-per-task

这在“请求会阻塞下游 I/O”时是很合理的，因为虚拟线程很擅长处理大量阻塞任务。

但在这次本地处理 profile 里，任务很短，结果就是：

- 每个请求都要有调度成本
- 还会带来虚拟线程对象分配

更细致的线程策略如下：

- 本地处理模式：固定大小平台线程池
- 需要下游转发的模式：虚拟线程池

该项预期收益低于切换 `binary`，但可减少短任务场景下的调度与分配成本。

### 9.4 第四优先级：减少响应构造时的短命对象

本地处理逻辑在 [ChainHandler.java](/Users/han/coding/janus/src/main/java/org/janus/handler/ChainHandler.java:351) 之后。

当前每次构造结果都会生成：

- `JanusMessage`
- `List`
- `Map`
- `UUID.randomUUID().toString()`

尤其是生成结果 id 的地方，会产生额外对象分配。

如果这个 `id` 只是演示数据，不是业务强约束，那么在高频压测模式下它并不划算。

可以考虑：

- 用递增 long 替代 UUID
- 或者直接复用已有 request id

该项属于收尾优化，不应作为首要改动。

---

## 10. 六类常见误判

### 10.1 误判一：`park` 很宽，所以锁有问题

不一定。

`park` 很宽，往往只是说明线程在等，而不是说明锁竞争严重。
需要结合上下文判断其等待对象：

- 等队列
- 等 I/O
- 等任务
- 等真正的锁

### 10.2 误判二：CPU 图里没看到业务方法，就说明业务没问题

业务可能通过“触发大量对象分配”的方式影响性能。
这时业务方法在 alloc 图里比在 CPU 图里更重要。

### 10.3 误判三：看到 Jackson 就下结论“换 Gson/Fastjson”

这一判断同样缺乏充分依据。问题未必在于某个 JSON 库，而可能是：

- 选择了文本协议
- 字段数量过多
- 对象层级过深
- 热路径不适合通用对象绑定

替换库可能带来收益，但通常不是首要原则。

### 10.4 误判四：把所有宽块都列成优化项

这种做法缺少优先级。分析应先区分：

- 这是“正常等待”还是“异常消耗”
- 这是“根因”还是“结果”

例如，本次 `SocketDispatcher.read0` 占比较高，不代表需要“优化 read0”。
它只是说明服务确实在做网络收发。

### 10.5 误判五：没做基线对比就改代码

可验证的方式如下：

1. 先做一次 profile
2. 提炼热点
3. 改一项
4. 再 profile 一次
5. 对比变化

否则无法验证改动是否有效。

### 10.6 误判六：把所有性能问题都归结为 GC

GC 往往是结果，不一定是根因。

这次如果 GC 压力大，真正的前因是：

- 大量 `char[]`
- 大量 `byte[]`
- 大量 `String`
- 大量 JSON 编解码中间对象

因此，应先分析分配热点。

---

## 11. 分析流程

任意一组火焰图均可按以下流程分析。

### 第一步：先分清楚图的类型

首先确认图的类型：

- 这是 CPU 图？
- 这是 alloc 图？
- 这是 lock 图？

不同图解决的问题不同。

### 第二步：先看最宽的 3 到 5 条路径

初始分析聚焦于主要路径，并回答：

- 最大头是等待、I/O、分配、锁，还是业务计算？

### 第三步：把热点分成三类

把热点按来源分组：

- 业务代码
- 三方库
- JVM/JDK 运行时

分组结果用于确定优化所在的层级。

### 第四步：把热点落回入口方法

例如这次就是：

- 入口：[JanusWsServer.onMessage](../src/main/java/org/janus/ws/JanusWsServer.java)
- 核心处理：[ChainHandler.handleJsonRequest](../src/main/java/org/janus/handler/ChainHandler.java)
- 模型：[JanusMessage](../src/main/java/org/janus/model/JanusMessage.java)

完成映射后，分析对象从图上的函数名转为代码中的真实路径。

### 第五步：先做结构性优化，再做局部优化

结构性优化例如：

- 换协议
- 改线程模型
- 改序列化路径

局部优化例如：

- 少分配一个 `Map`
- 少创建一个 `StringBuilder`
- 少调一次 `UUID.randomUUID()`

结构性优化优先于局部优化。

### 第六步：改动后重新采样

每轮改动后均应重新生成火焰图，并确认：

- 原热点是否真的缩小了
- 有没有把问题转移到别的地方

---

## 12. 一句话总结这个案例

本次 Janus 火焰图可总结为：

> 当前热点并非业务逻辑耗尽 CPU，而是 WebSocket JSON 路径上的 Jackson 编解码和文本帧转换制造了大量短命对象。因此，优化重点是减少 JSON、字符串和缓冲区分配，而非优先进行 CPU 微优化。

这一结论由 CPU 等待路径与 JSON 文本链路的分配热点共同支撑。

---

## 附：本文对应的关键代码位置

- WebSocket JSON 入口：[src/main/java/org/janus/ws/JanusWsServer.java](../src/main/java/org/janus/ws/JanusWsServer.java)
- JSON 请求处理：[src/main/java/org/janus/handler/ChainHandler.java](../src/main/java/org/janus/handler/ChainHandler.java)
- 统一消息模型：[src/main/java/org/janus/model/JanusMessage.java](../src/main/java/org/janus/model/JanusMessage.java)
- 执行器策略：[src/main/java/org/janus/common/ExecutorSupport.java](../src/main/java/org/janus/common/ExecutorSupport.java)
- Profiling 流程说明：[doc/arthas-profiler.md](arthas-profiler.md)
- 本次使用的火焰图：
  - [`cpu-30s.html`](../arthas-output/20260715-160848-pid-13947/cpu-30s.html)
  - [`memory-alloc-30s.html`](../arthas-output/20260715-160848-pid-13947/memory-alloc-30s.html)

后续分析可重复执行本文的采集、定位、改动和复测流程，以验证优化收益。
