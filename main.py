"""修复 unique_session 会话隔离导致的多处内存泄漏（插件层，不改 AstrBot 本体）。

问题与泄漏原理:
1. active_event_registry 事件对象常驻残留:
   - scheduler.execute() 先按原始 umo（如 aiocqhttp:GroupMessage:123）注册事件；
   - WakingCheckStage 在开启 unique_session 时改写 event.session_id（如 user_123）；
   - finally 中 unregister(event) 用改写后的 new_umo 注销，导致旧 key 下的事件对象永久残留；
   - 同时，用户发送 /stop 或 /reset 是以 new_umo 查表，无法停止注册在 old_umo 下的活跃事件。
2. GroupChatContext（群聊上下文感知）队列无界膨胀:
   - handle_message / on_req_llm 使用 event.unified_msg_origin 作为 key；
   - 开启会话隔离后，群内每个发言群友均生成一个独立的 raw_records 队列（上限 1000 条）；
   - 普通群友从不唤醒大模型，其队列永久保留且不断积压，产生线性内存膨胀；且群聊上下文感知被割裂。
3. RateLimitStage（限流阶段）锁与空队列无界累积:
   - event_timestamps 与 locks 字典对每个发言用户常驻保存；
   - 时间戳过期后，空 deque 与闲置的 asyncio.Lock 从不主动释放，随群友发言量累积。

修复策略（纯插件层 monkey-patch，terminate 完整还原）:
1. AstrMessageEvent.session_id / unified_msg_origin setter 包装:
   - 当 session_id 发生变更时，自动将 active_event_registry 中旧 key 下的事件对象原子迁移至新 key；
   - unregister 时双向清理新旧 key，并在集合为空时 pop 键；
   - stop_all / request_agent_stop_all 支持联合匹配，确保隔离会话与群组均能精准响应中断。
2. GroupChatContext 规范化群 UMO 代理:
   - 群消息接入时，使用 _EventProxy 将 context 内部的 umo 归一化为原生群 UMO（platform:GroupMessage:group_id）；
   - 多个群友的群聊上下文汇聚在统一群队列中，LLM 唤醒时正常消费，彻底消除用户级队列膨胀。
3. RateLimitStage 自动清理与实例跟踪:
   - 跟踪 RateLimitStage 实例，周期回收过期时间戳、移除空 deque 并释放无锁的 asyncio.Lock。
4. 周期守护与手动指令:
   - 默认每 10 分钟自动执行一次全量存量清扫；
   - 提供管理员指令 /fixreg，支持即时手动清理与多模块诊断报告。
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime
import gc
import os
from typing import Any
import weakref

from astrbot.api import logger, star
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.platform import MessageType
from astrbot.api.star import register

_REGISTRY_KEY_EXTRA = "_active_event_registry_key"
_PATCHED_FLAG = "_event_registry_fix_patched"


def _get_canonical_group_umo(event: AstrMessageEvent) -> str:
    """获取事件的原生群组 UMO（不受 unique_session 隔离影响）。"""
    try:
        if event.get_message_type() == MessageType.GROUP_MESSAGE:
            group_id = event.get_group_id()
            if group_id:
                platform_id = event.get_platform_id()
                msg_type = (
                    event.session.message_type.value
                    if hasattr(event, "session")
                    and hasattr(event.session, "message_type")
                    else "GroupMessage"
                )
                return f"{platform_id}:{msg_type}:{group_id}"
    except Exception:
        pass
    return event.unified_msg_origin


class _EventProxy:
    """代理事件对象，将 unified_msg_origin 透明重定向到原生群组 UMO，供 GroupChatContext 使用。"""

    def __init__(self, target: Any, group_umo: str) -> None:
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_group_umo", group_umo)

    @property
    def __class__(self):
        return self._target.__class__

    def __getattr__(self, name: str) -> Any:
        if name == "unified_msg_origin":
            return object.__getattribute__(self, "_group_umo")
        return getattr(object.__getattribute__(self, "_target"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ("_target", "_group_umo"):
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, "_target"), name, value)


@register(
    "astrbot_event_registry_fix",
    "workbuddy",
    "修复 unique_session 会话隔离导致的 active_event_registry、GroupChatContext 及 RateLimit 内存泄漏",
    "1.1.0",
)
class EventRegistryFix(star.Star):
    def __init__(self, context: star.Context) -> None:
        super().__init__(context)
        # active_event_registry 引用与原方法
        self._registry = None
        self._orig_register = None
        self._orig_unregister = None
        self._orig_stop_all = None
        self._orig_request_agent_stop_all = None

        # AstrMessageEvent 属性描述符原对象
        self._orig_session_id_prop = None
        self._orig_umo_prop = None

        # GroupChatContext 原方法
        self._gcc_cls = None
        self._orig_gcc_handle_message = None
        self._orig_gcc_on_req_llm = None
        self._orig_gcc_remove_session = None
        self._orig_gcc_need_active_reply = None

        # RateLimitStage 跟踪与原方法
        self._rl_cls = None
        self._orig_rl_init = None
        self._orig_rl_process = None
        self._rl_instances: weakref.WeakSet = weakref.WeakSet()

        self._sweep_task: asyncio.Task | None = None
        self._patched = False

    async def initialize(self) -> None:
        try:
            from astrbot.core.utils.active_event_registry import (
                active_event_registry,
            )

            self._registry = active_event_registry
        except Exception as e:  # noqa: BLE001
            logger.error(f"[EventRegistryFix] 无法导入 active_event_registry: {e}")
            return

        self._apply_patches()
        self._sweep_task = asyncio.create_task(self._sweep_loop())
        logger.info("[EventRegistryFix] 会话隔离多模块内存泄漏修复已成功挂载。")

    # ── 猴子补丁应用与还原 ──────────────────────────────────────

    def _apply_patches(self) -> None:
        reg = self._registry
        if getattr(reg, _PATCHED_FLAG, False):
            return

        # 1. Patch active_event_registry 方法
        self._orig_register = reg.register
        self._orig_unregister = reg.unregister
        self._orig_stop_all = reg.stop_all
        self._orig_request_agent_stop_all = reg.request_agent_stop_all

        def patched_register(event: AstrMessageEvent, _orig=self._orig_register):
            try:
                event.set_extra(_REGISTRY_KEY_EXTRA, event.unified_msg_origin)
            except Exception:  # noqa: BLE001
                pass
            return _orig(event)

        def patched_unregister(event: AstrMessageEvent, _orig=self._orig_unregister):
            try:
                reg._agent_stop_callbacks.pop(event, None)
                keys_to_clean: set[str] = set()
                extra_key = event.get_extra(_REGISTRY_KEY_EXTRA)
                if extra_key:
                    keys_to_clean.add(extra_key)
                try:
                    keys_to_clean.add(event.unified_msg_origin)
                except Exception:  # noqa: BLE001
                    pass

                for k in keys_to_clean:
                    events_set = reg._events.get(k)
                    if events_set is not None:
                        events_set.discard(event)
                        if not events_set:
                            reg._events.pop(k, None)

                # 防御性全量清除，确保该事件对象在任何 key 下都不残留
                for k, s in list(reg._events.items()):
                    if event in s:
                        s.discard(event)
                        if not s:
                            reg._events.pop(k, None)
                return
            except Exception:  # noqa: BLE001
                return _orig(event)

        def _collect_matching_umos(target_umo: str) -> set[str]:
            matched = {target_umo}
            parts = target_umo.split(":")
            if len(parts) == 3 and parts[1] == "GroupMessage":
                group_id = parts[2]
                all_keys = set(reg._events.keys()) | set(reg._background_tasks.keys())
                suffix1 = f"_{group_id}"
                suffix2 = f"%{group_id}"
                for k in all_keys:
                    if k.startswith(f"{parts[0]}:GroupMessage:"):
                        k_sid = k.split(":", 2)[2]
                        if k_sid.endswith(suffix1) or k_sid.endswith(suffix2):
                            matched.add(k)
            return matched

        def patched_stop_all(
            umo: str,
            exclude: AstrMessageEvent | None = None,
            _orig=self._orig_stop_all,
        ) -> int:
            matched = _collect_matching_umos(umo)
            all_events = set()
            for m_umo in matched:
                all_events.update(reg._events.get(m_umo, []))
                all_events.update(
                    ev for task, ev in reg._background_tasks.get(m_umo, {}).items()
                    if not task.done()
                )
            for ev in all_events:
                if ev is not exclude:
                    ev.stop_event()
            return reg.request_agent_stop_all(umo, exclude)

        def patched_request_agent_stop_all(
            umo: str,
            exclude: AstrMessageEvent | None = None,
            _orig=self._orig_request_agent_stop_all,
        ) -> int:
            matched = _collect_matching_umos(umo)
            all_events = set()
            all_tasks: dict[Any, Any] = {}
            for m_umo in matched:
                all_events.update(reg._events.get(m_umo, []))
                tasks = reg._background_tasks.get(m_umo, {})
                all_tasks.update(tasks)
                all_events.update(ev for t, ev in tasks.items() if not t.done())

            count = 0
            for event in all_events:
                if event is not exclude:
                    event.set_extra("agent_stop_requested", True)
                    reg.get_background_stop_signal(event).set()
                    callback = reg._agent_stop_callbacks.get(event)
                    if callback:
                        callback()
                    count += 1

            for task, event in list(all_tasks.items()):
                if (
                    event is not exclude
                    and not task.done()
                    and task not in reg._background_cancel_requested
                ):
                    reg._background_cancel_requested.add(task)
                    task.cancel()
            return count

        reg.register = patched_register
        reg.unregister = patched_unregister
        reg.stop_all = patched_stop_all
        reg.request_agent_stop_all = patched_request_agent_stop_all

        # 2. Patch AstrMessageEvent 属性描述符（捕获会话改写并原子迁移）
        try:
            from astrbot.core.platform.astr_message_event import AstrMessageEvent as AMECls

            self._orig_session_id_prop = AMECls.session_id
            self._orig_umo_prop = AMECls.unified_msg_origin

            orig_sid_fget = self._orig_session_id_prop.fget
            orig_sid_fset = self._orig_session_id_prop.fset
            orig_umo_fget = self._orig_umo_prop.fget
            orig_umo_fset = self._orig_umo_prop.fset

            def _handle_event_umo_changed(event: AstrMessageEvent, old_umo: str, new_umo: str):
                if old_umo == new_umo or not self._registry:
                    return
                try:
                    event.set_extra(_REGISTRY_KEY_EXTRA, new_umo)
                except Exception:  # noqa: BLE001
                    pass

                events_map = getattr(self._registry, "_events", None)
                if isinstance(events_map, dict):
                    old_set = events_map.get(old_umo)
                    if old_set and event in old_set:
                        old_set.discard(event)
                        if not old_set:
                            events_map.pop(old_umo, None)
                        events_map[new_umo].add(event)

                bg_tasks = getattr(self._registry, "_background_tasks", None)
                if isinstance(bg_tasks, dict):
                    old_bg = bg_tasks.get(old_umo)
                    if old_bg:
                        tasks_to_move = [t for t, ev in old_bg.items() if ev is event]
                        if tasks_to_move:
                            for t in tasks_to_move:
                                del old_bg[t]
                                bg_tasks[new_umo][t] = event
                            if not old_bg:
                                bg_tasks.pop(old_umo, None)

            def patched_sid_fset(event_self: AstrMessageEvent, value: str):
                old_umo = event_self.unified_msg_origin
                orig_sid_fset(event_self, value)
                new_umo = event_self.unified_msg_origin
                if old_umo != new_umo:
                    _handle_event_umo_changed(event_self, old_umo, new_umo)

            def patched_umo_fset(event_self: AstrMessageEvent, value: str):
                old_umo = event_self.unified_msg_origin
                orig_umo_fset(event_self, value)
                new_umo = event_self.unified_msg_origin
                if old_umo != new_umo:
                    _handle_event_umo_changed(event_self, old_umo, new_umo)

            AMECls.session_id = property(
                orig_sid_fget,
                patched_sid_fset,
                self._orig_session_id_prop.fdel,
                self._orig_session_id_prop.__doc__,
            )
            AMECls.unified_msg_origin = property(
                orig_umo_fget,
                patched_umo_fset,
                self._orig_umo_prop.fdel,
                self._orig_umo_prop.__doc__,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[EventRegistryFix] AstrMessageEvent 属性 hook 挂载失败: {e}")

        # 3. Patch GroupChatContext 类（规范化群聊 UMO，汇聚队列并消除膨胀）
        try:
            from astrbot.builtin_stars.astrbot.group_chat_context import GroupChatContext

            self._gcc_cls = GroupChatContext
            self._orig_gcc_handle_message = GroupChatContext.handle_message
            self._orig_gcc_on_req_llm = GroupChatContext.on_req_llm
            self._orig_gcc_remove_session = GroupChatContext.remove_session
            self._orig_gcc_need_active_reply = GroupChatContext.need_active_reply

            async def patched_gcc_handle_message(
                gcc_self: GroupChatContext,
                event: AstrMessageEvent,
                _orig=self._orig_gcc_handle_message,
            ):
                group_umo = _get_canonical_group_umo(event)
                proxy = (
                    _EventProxy(event, group_umo)
                    if group_umo != event.unified_msg_origin
                    else event
                )
                return await _orig(gcc_self, proxy)

            async def patched_gcc_on_req_llm(
                gcc_self: GroupChatContext,
                event: AstrMessageEvent,
                req: Any,
                _orig=self._orig_gcc_on_req_llm,
            ):
                group_umo = _get_canonical_group_umo(event)
                proxy = (
                    _EventProxy(event, group_umo)
                    if group_umo != event.unified_msg_origin
                    else event
                )
                return await _orig(gcc_self, proxy, req)

            async def patched_gcc_remove_session(
                gcc_self: GroupChatContext,
                event: AstrMessageEvent,
                _orig=self._orig_gcc_remove_session,
            ):
                group_umo = _get_canonical_group_umo(event)
                proxy = (
                    _EventProxy(event, group_umo)
                    if group_umo != event.unified_msg_origin
                    else event
                )
                return await _orig(gcc_self, proxy)

            async def patched_gcc_need_active_reply(
                gcc_self: GroupChatContext,
                event: AstrMessageEvent,
                _orig=self._orig_gcc_need_active_reply,
            ):
                group_umo = _get_canonical_group_umo(event)
                proxy = (
                    _EventProxy(event, group_umo)
                    if group_umo != event.unified_msg_origin
                    else event
                )
                return await _orig(gcc_self, proxy)

            GroupChatContext.handle_message = patched_gcc_handle_message
            GroupChatContext.on_req_llm = patched_gcc_on_req_llm
            GroupChatContext.remove_session = patched_gcc_remove_session
            GroupChatContext.need_active_reply = patched_gcc_need_active_reply
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[EventRegistryFix] GroupChatContext hook 挂载失败: {e}")

        # 4. Patch RateLimitStage 类（跟踪实例并在容量过大时机会性收敛）
        try:
            from astrbot.core.pipeline.rate_limit_check.stage import RateLimitStage

            self._rl_cls = RateLimitStage
            self._orig_rl_init = RateLimitStage.__init__
            self._orig_rl_process = RateLimitStage.process

            # 载入已有存量实例
            for obj in gc.get_objects():
                if isinstance(obj, RateLimitStage):
                    self._rl_instances.add(obj)

            def patched_rl_init(rl_self: RateLimitStage, *args, **kwargs):
                self._orig_rl_init(rl_self, *args, **kwargs)
                self._rl_instances.add(rl_self)

            async def patched_rl_process(
                rl_self: RateLimitStage,
                event: AstrMessageEvent,
                _orig=self._orig_rl_process,
            ):
                res = await _orig(rl_self, event)
                # 当会话过多时进行机会性清扫
                if len(rl_self.event_timestamps) > 100:
                    self._clean_single_rate_limit_stage(rl_self)
                return res

            RateLimitStage.__init__ = patched_rl_init
            RateLimitStage.process = patched_rl_process
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[EventRegistryFix] RateLimitStage hook 挂载失败: {e}")

        setattr(reg, _PATCHED_FLAG, True)
        self._patched = True

    def _remove_patches(self) -> None:
        reg = self._registry
        if reg is not None and self._patched:
            reg.register = self._orig_register
            reg.unregister = self._orig_unregister
            reg.stop_all = self._orig_stop_all
            reg.request_agent_stop_all = self._orig_request_agent_stop_all
            setattr(reg, _PATCHED_FLAG, False)

        try:
            from astrbot.core.platform.astr_message_event import AstrMessageEvent as AMECls

            if self._orig_session_id_prop:
                AMECls.session_id = self._orig_session_id_prop
            if self._orig_umo_prop:
                AMECls.unified_msg_origin = self._orig_umo_prop
        except Exception:  # noqa: BLE001
            pass

        if self._gcc_cls:
            self._gcc_cls.handle_message = self._orig_gcc_handle_message
            self._gcc_cls.on_req_llm = self._orig_gcc_on_req_llm
            self._gcc_cls.remove_session = self._orig_gcc_remove_session
            self._gcc_cls.need_active_reply = self._orig_gcc_need_active_reply

        if self._rl_cls:
            self._rl_cls.__init__ = self._orig_rl_init
            self._rl_cls.process = self._orig_rl_process

        self._patched = False

    # ── 存量清扫逻辑 ───────────────────────────────────────

    def _sweep_active_event_registry(self) -> int:
        """清理 active_event_registry 中残留无效事件。返回清理数量。"""
        reg = self._registry
        if reg is None:
            return 0
        removed = 0
        events_map = getattr(reg, "_events", None)
        if isinstance(events_map, dict):
            for umo in list(events_map.keys()):
                events_set = events_map.get(umo)
                if not events_set:
                    events_map.pop(umo, None)
                    continue
                for event in list(events_set):
                    try:
                        curr_umo = event.unified_msg_origin
                        extra_umo = event.get_extra(_REGISTRY_KEY_EXTRA)
                    except Exception:  # noqa: BLE001
                        events_set.discard(event)
                        removed += 1
                        continue
                    if curr_umo != umo and extra_umo != umo:
                        events_set.discard(event)
                        removed += 1
                if not events_set:
                    events_map.pop(umo, None)

        bg_tasks = getattr(reg, "_background_tasks", None)
        if isinstance(bg_tasks, dict):
            for umo in list(bg_tasks.keys()):
                task_dict = bg_tasks.get(umo)
                if task_dict:
                    for task in list(task_dict.keys()):
                        if task.done():
                            task_dict.pop(task, None)
                if not task_dict:
                    bg_tasks.pop(umo, None)

        return removed

    def _get_all_group_chat_contexts(self) -> list[Any]:
        contexts = []
        try:
            from astrbot.core.star.star import star_registry

            for meta in star_registry:
                if meta.star_cls:
                    gcc = getattr(meta.star_cls, "group_chat_context", None)
                    if gcc is not None and gcc not in contexts:
                        contexts.append(gcc)
        except Exception:  # noqa: BLE001
            pass

        if not contexts and self._gcc_cls:
            for obj in gc.get_objects():
                if isinstance(obj, self._gcc_cls) and obj not in contexts:
                    contexts.append(obj)
        return contexts

    def _clean_group_chat_contexts(self) -> dict[str, int]:
        """合并/清理 GroupChatContext 中因会话隔离留存的用户队列与孤儿锁。"""
        total_isolated = 0
        total_empty = 0
        total_locks = 0

        contexts = self._get_all_group_chat_contexts()
        for gcc in contexts:
            # 1. 扫描 raw_records
            for umo in list(gcc.raw_records.keys()):
                records = gcc.raw_records.get(umo)
                if not records:
                    gcc.raw_records.pop(umo, None)
                    gcc._record_ids.pop(umo, None)
                    total_empty += 1
                    continue

                # 识别由 unique_session 生成的带分隔符 key 并尝试归并入群 UMO
                parts = umo.split(":")
                if len(parts) == 3 and parts[1] == "GroupMessage":
                    sid = parts[2]
                    group_id = None
                    if "%" in sid:
                        group_id = sid.split("%", 1)[1]
                    elif "_" in sid:
                        group_id = sid.rsplit("_", 1)[1]

                    if group_id:
                        canonical_umo = f"{parts[0]}:GroupMessage:{group_id}"
                        if canonical_umo != umo:
                            can_records = gcc.raw_records[canonical_umo]
                            can_ids = gcc._record_ids[canonical_umo]
                            rec_ids = gcc._record_ids.get(umo, deque())
                            for r, rid in zip(records, rec_ids):
                                if r not in can_records:
                                    can_records.append(r)
                                    can_ids.append(rid)
                            while len(can_records) > 1000:
                                can_records.popleft()
                                if can_ids:
                                    can_ids.popleft()
                            gcc.raw_records.pop(umo, None)
                            gcc._record_ids.pop(umo, None)
                            total_isolated += 1

            # 2. 清理未在使用且已解锁的锁
            for umo in list(gcc._locks.keys()):
                lock = gcc._locks.get(umo)
                if lock is not None and not lock.locked() and umo not in gcc.raw_records:
                    gcc._locks.pop(umo, None)
                    total_locks += 1

        return {
            "isolated_keys": total_isolated,
            "empty_keys": total_empty,
            "locks": total_locks,
        }

    def _clean_single_rate_limit_stage(
        self,
        stage: Any,
        now: datetime | None = None,
    ) -> dict[str, int]:
        if now is None:
            now = datetime.now()
        cleaned_ts = 0
        cleaned_locks = 0

        for umo in list(stage.event_timestamps.keys()):
            ts = stage.event_timestamps.get(umo)
            if ts is not None:
                stage._remove_expired_timestamps(ts, now)
                if not ts:
                    stage.event_timestamps.pop(umo, None)
                    cleaned_ts += 1
                    lock = stage.locks.get(umo)
                    if lock is not None and not lock.locked():
                        stage.locks.pop(umo, None)
                        cleaned_locks += 1

        for umo in list(stage.locks.keys()):
            if umo not in stage.event_timestamps:
                lock = stage.locks.get(umo)
                if lock is not None and not lock.locked():
                    stage.locks.pop(umo, None)
                    cleaned_locks += 1

        return {"timestamps": cleaned_ts, "locks": cleaned_locks}

    def _clean_rate_limit_stages(self) -> dict[str, int]:
        """清理所有活跃 RateLimitStage 中的过期时间戳与空闲锁。"""
        now = datetime.now()
        total_ts = 0
        total_locks = 0
        stages = list(self._rl_instances)
        if not stages and self._rl_cls:
            for obj in gc.get_objects():
                if isinstance(obj, self._rl_cls) and obj not in stages:
                    self._rl_instances.add(obj)
                    stages.append(obj)

        for stage in stages:
            res = self._clean_single_rate_limit_stage(stage, now)
            total_ts += res["timestamps"]
            total_locks += res["locks"]

        return {"timestamps": total_ts, "locks": total_locks}

    def _sweep_once(self) -> dict[str, Any]:
        """执行全模块统一清扫。"""
        reg_removed = self._sweep_active_event_registry()
        gcc_res = self._clean_group_chat_contexts()
        rl_res = self._clean_rate_limit_stages()
        collected = gc.collect()
        return {
            "reg_removed": reg_removed,
            "gcc_isolated": gcc_res["isolated_keys"],
            "gcc_empty": gcc_res["empty_keys"],
            "gcc_locks": gcc_res["locks"],
            "rl_timestamps": rl_res["timestamps"],
            "rl_locks": rl_res["locks"],
            "gc_collected": collected,
        }

    async def _sweep_loop(self, interval_sec: int = 600) -> None:
        try:
            while True:
                await asyncio.sleep(interval_sec)
                res = self._sweep_once()
                total_cleaned = (
                    res["reg_removed"]
                    + res["gcc_isolated"]
                    + res["gcc_empty"]
                    + res["gcc_locks"]
                    + res["rl_timestamps"]
                    + res["rl_locks"]
                )
                if total_cleaned > 0:
                    logger.info(
                        f"[EventRegistryFix] 周期清扫完成: 移除事件 {res['reg_removed']} 个, "
                        f"群上下文清理(隔离队列:{res['gcc_isolated']}, 锁:{res['gcc_locks']}), "
                        f"限流器清理(过期时间戳:{res['rl_timestamps']}, 锁:{res['rl_locks']}), "
                        f"GC回收对象 {res['gc_collected']} 个。"
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.error(f"[EventRegistryFix] 清扫任务异常: {e}")

    async def terminate(self) -> None:
        if self._sweep_task:
            self._sweep_task.cancel()
            self._sweep_task = None
        self._remove_patches()
        self._rl_instances.clear()
        gc.collect()
        logger.info("[EventRegistryFix] 已还原所有模块补丁并退出。")

    # ── 管理员指令 ──────────────────────────────────────────

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("fixreg")
    async def fixreg(self, event: AstrMessageEvent):
        """手动清理 unique_session 导致的所有残留对象与内存泄漏（仅管理员）。"""
        reg = self._registry
        if reg is None:
            yield event.plain_result("[EventRegistryFix] 注册表未就绪，插件未正确加载。")
            return

        # 统计清理前状态
        before_reg_events = sum(len(s) for s in reg._events.values()) if hasattr(reg, "_events") else 0
        before_reg_keys = len(reg._events) if hasattr(reg, "_events") else 0

        # 获取 RSS
        rss_before = -1.0
        try:
            import psutil

            rss_before = psutil.Process(os.getpid()).memory_info().rss / 1048576
        except Exception:  # noqa: BLE001
            pass

        # 执行全量清理
        res = self._sweep_once()

        # 统计清理后状态
        after_reg_events = sum(len(s) for s in reg._events.values()) if hasattr(reg, "_events") else 0
        after_reg_keys = len(reg._events) if hasattr(reg, "_events") else 0

        rss_after = -1.0
        try:
            import psutil

            rss_after = psutil.Process(os.getpid()).memory_info().rss / 1048576
        except Exception:  # noqa: BLE001
            pass

        rss_text = ""
        if rss_before >= 0 and rss_after >= 0:
            diff = rss_after - rss_before
            sign = "+" if diff >= 0 else ""
            rss_text = f"\n内存 RSS: {rss_before:.1f} MB -> {rss_after:.1f} MB ({sign}{diff:.1f} MB)"

        yield event.plain_result(
            f"⚡ 会话隔离内存泄漏清扫完毕：\n"
            f"1. 活跃事件注册表: {before_reg_events} -> {after_reg_events} 个事件, "
            f"{before_reg_keys} -> {after_reg_keys} 个 key (移除残留: {res['reg_removed']})\n"
            f"2. 群聊上下文感知: 合并隔离队列 {res['gcc_isolated']} 个, 清理空队列 {res['gcc_empty']} 个, 释放闲置锁 {res['gcc_locks']} 个\n"
            f"3. 频率限制组件: 回收过期时间戳 {res['rl_timestamps']} 个, 释放限流锁 {res['rl_locks']} 个\n"
            f"4. Python GC: 回收无主对象 {res['gc_collected']} 个{rss_text}"
        )
