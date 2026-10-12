# astrbot_event_registry_fix

修复 AstrBot 在开启 `unique_session`（会话隔离）后，`active_event_registry` 中事件对象按「改写前 umo」永久残留的内存泄漏。**纯修复插件：仅打补丁，无任何清理指令或后台任务。**

## 问题背景

开启 `platform_settings.unique_session` 后，每一条群消息都会向 `active_event_registry._events` 泄漏一个完整的 `AstrMessageEvent` 对象（含消息链、平台引用、extras 等），且永远不会被释放，长期运行内存单调增长直至 OOM。

泄漏原因：

- `PipelineScheduler.execute()` 在管道执行前按当时的 `event.unified_msg_origin` 注册事件，结束后调用 `unregister(event)`；
- 但 `unregister` 内部会**重新计算** `unified_msg_origin`（它是一个动态属性，返回 `str(self.session)`）；
- 管道第一阶段 `WakingCheckStage` 在会话隔离开启时会改写 `event.session_id`，导致注销时的 key 已经变成 `平台:GroupMessage:群号_用户ID`，而注册时的 key 是 `平台:GroupMessage:群号`；
- `discard` 在新 key 的集合里什么也删不到，旧 key 集合中的事件对象永久残留。

详细分析见 issue：`astrbot_issue_event_leak.md`。

## 修复原理

在插件层对单例 `active_event_registry` 做最小化包装（不改 AstrBot 本体）：

1. **包装 `register`**：注册前把当时的 `unified_msg_origin` 快照存进 `event.set_extra("_active_event_registry_key", ...)`；
2. **包装 `unregister`**：优先使用注册时保存的快照 key 做 `discard`，改用 `get` + `pop` 避免向 `defaultdict` 插入空集合；异常时兜底回落到原实现，保证注销动作绝不缺失；
3. 插件 `terminate`（卸载/重载）时完整还原原始方法，通过 `_PATCHED_FLAG` 防止重复打补丁。

补丁生效后，事件注册与注销使用同一个 key，泄漏即被消除。**对于插件加载之前已经积累的存量残留事件**，它们会随进程重启而释放（或等待官方本体修复）；本插件不再提供运行时清扫。

## 使用方式

1. 将本目录放入 AstrBot 的 `data/plugins/` 下（或通过 WebUI 从本地导入）；
2. 在 WebUI 插件管理中启用 `astrbot_event_registry_fix`；
3. 无任何配置项和指令，加载即生效。

## 注意事项

- 本插件通过实例属性覆盖单例方法实现 patch，**仅影响 `active_event_registry` 单例**，不影响类定义和其他模块；
- 若官方在后续版本采纳了本体修复，本插件即可直接卸载（卸载时自动还原，无残留影响）；
- 若 AstrBot 未来修改了 `active_event_registry` 的内部结构（如 `_events`、`_agent_stop_callbacks` 属性名），补丁可能失效并回落到原实现，届时需更新本插件。

## 环境

- AstrBot v4.9.2 上开发验证（泄漏逻辑与版本无关，历史版本同样适用）
- Python 3.10+（使用了 `X | None` 类型注解）
