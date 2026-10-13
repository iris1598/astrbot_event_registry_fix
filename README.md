# astrbot_event_registry_fix

> 专为 AstrBot 设计的**会话隔离（`unique_session`）内存泄漏综合修复插件**。  
> **核心优势**：纯插件层动态猴子补丁（Monkey-Patch），**完全不修改 AstrBot 本体代码**，即插即用，支持热重载与安全卸载。

---

## 📌 背景与问题定位

在 AstrBot 的平台配置中，开启群聊会话隔离（`platform_settings.unique_session: true`）能让群内每位成员拥有独立的大模型会话历史。但由于底层核心调度管线与组件未能完全适配 UMO（Unified Message Origin）的动态突变，会导致严重的内存泄漏与控制失效：

### 1. 活跃事件注册表事件残留 & `/stop` 中断失效（高危）
- **事件对象泄漏**：`PipelineScheduler.execute()` 在进入流水线前使用原始 UMO（如 `aiocqhttp:GroupMessage:123456`）注册事件；流水线第一阶段 `WakingCheckStage` 将 `event.session_id` 改写为隔离 ID（如 `user999_123456`）；执行完毕后在 `finally` 中使用改写后的 UMO 注销。导致旧 key 集合中的事件对象**永久残留**，每条群消息都会泄漏一个完整的事件对象（携带消息链、媒体文件引用、TraceSpan 等），内存随对话量线性暴增。
- **`/stop` 无法中断**：用户在隔离会话下发送 `/stop` 或 `/reset` 指令时是以隔离 UMO 查表，无法匹配到注册在旧 key 下的正在运行事件，导致活跃 Agent 无法被正常停止。

### 2. 群聊上下文感知（`GroupChatContext`）消息队列无界膨胀（严重）
- **队列常驻不销毁**：群聊上下文感知功能（`group_icl_enable`）直接按 `event.unified_msg_origin` 维护消息队列（上限 1000 条）。开启会话隔离后，群内**每个发言的群友**都会创建独立的 `raw_records` 队列。
- **无法消费与上下文割裂**：绝大多数群友从不唤醒大模型，其队列**永远不会被消费清空**，导致几百上千个用户的聊天记录常驻内存；同时，当某位用户唤醒大模型时，大模型只能读取该用户自身的发言记录，无法感知群内其他群友的聊天内容，功能逻辑严重割裂。

### 3. 限流组件（`RateLimitStage`）锁与空队列常驻累积（中危）
- 限流组件内部使用 `locks`（`asyncio.Lock`）与 `event_timestamps`（`deque[datetime]`）管理会话。
- 时间戳过期被移出队列后，**空 `deque` 和闲置的 `asyncio.Lock` 从不主动释放**。在开启隔离的活跃群中，成千上万个发言群友的锁对象与键常驻堆积。

---

## 🛠️ 修复原理与实现特性

本插件在不侵入修改 AstrBot 源码的前提下，通过精准的 Python 描述符拦截与动态代理机制进行热修复：

| 模块 | 修复前现象 | 插件修复机制 |
| :--- | :--- | :--- |
| **`active_event_registry`** | 事件残留泄漏、`/stop` 失效 | 包装 `AstrMessageEvent.session_id` 属性 setter，在会话 ID 改写时**原子迁移事件与后台任务**至新 key；`unregister` 双向清理并释放空键；`stop_all` 支持群级与隔离会话联合去重匹配。 |
| **`GroupChatContext`** | 每人一份 1000 条队列，永不释放 | 采用 `_EventProxy` 透明代理，将群消息在内部统一重定向归一化为**原生群 UMO**（`platform:GroupMessage:group_id`）。群成员发言汇聚在单一群队列中，LLM 唤醒时正常消费，恢复真正的群聊感知并彻底杜绝膨胀。 |
| **`RateLimitStage`** | 空队列与闲置锁永久留存 | 实例弱引用追踪（`WeakSet`），在会话数较多时执行机会性收敛；周期清扫过期时间戳、移除空 `deque` 并释放已解锁的 `asyncio.Lock`。 |
| **存量数据与 GC 回收** | 插件加载前已累积的历史泄漏 | 周期守护任务（默认 10 分钟）全量清扫存量残留，并将历史遗留的单人隔离记录平滑归并至原生群队列中，配合 `gc.collect()` 加速内存释放。 |
| **插件生命周期** | 担心猴子补丁残留污染 | `terminate()` 中实现 **100% 完整原状还原**（包括属性描述符、实例方法、类方法），卸载或重载插件不留任何副作用。 |

---

## 💻 管理员指令

插件提供即时诊断与清扫指令（仅管理员可用）：

### `/fixreg`
立即触发一次全模块深度清扫并执行 Python 垃圾回收，返回详细的处理报告与内存指标：

```text
⚡ 会话隔离内存泄漏清扫完毕：
1. 活跃事件注册表: 52 -> 0 个事件, 12 -> 0 个 key (移除残留: 52)
2. 群聊上下文感知: 合并隔离队列 18 个, 清理空队列 5 个, 释放闲置锁 23 个
3. 频率限制组件: 回收过期时间戳 40 个, 释放限流锁 40 个
4. Python GC: 回收无主对象 138 个
内存 RSS: 245.2 MB -> 186.8 MB (-58.4 MB)
```

---

## 📦 安装与使用

1. 将本插件目录放入 AstrBot 的插件目录：
   ```text
   AstrBot/data/plugins/astrbot_event_registry_fix
   ```
2. 目录结构：
   ```text
   astrbot_event_registry_fix/
   ├── README.md         # 插件说明文档
   ├── metadata.yaml     # 插件元数据（v1.1.0）
   └── main.py           # 插件核心逻辑与补丁
   ```
3. 重启 AstrBot，或在管理面板重载插件，控制台输出如下日志即表示修复已成功挂载：
   ```text
   [EventRegistryFix] 会话隔离多模块内存泄漏修复已成功挂载。
   ```

---

## ⚙️ 常见问题 (FAQ)

**Q：使用这个插件需要修改 AstrBot 本体代码吗？**  
A：完全不需要。本插件完全运行在插件层，通过标准 API 和 Python 运行时钩子进行无侵入式注入与还原。

**Q：如果我在后台关闭了 `unique_session`，插件会有副作用吗？**  
A：不会。当没有开启会话隔离时，`session_id` 不会发生二次重写，所有 hook 均会透明直通原生逻辑，无任何负面影响。

**Q：为什么执行 `/fixreg` 后 RSS 物理内存没有立刻大幅下降？**  
A：Python 底层向操作系统归还物理内存受操作系统的内存分配器（如 glibc malloc / Windows Heap）策略影响。只要残留对象已被清理，空闲内存就会进入 Python 内部内存池，供后续消息处理复用，从而彻底遏制内存持续上涨。
