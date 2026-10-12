"""修复 active_event_registry 的内存泄漏（插件层，不改 AstrBot 本体）。

泄漏原理:
  scheduler.execute() 先按 event.unified_msg_origin（当时的值）注册事件，
  但管道第一阶段 waking_check 在开启 platform_settings.unique_session 时会改写
  event.session_id（会话隔离），导致 finally 中 unregister(event) 用「改写后的
  umo」做 key 去 discard —— 旧 key 集合里的这个事件对象永久残留。
  结果：每条群消息泄漏一个完整事件对象（含消息链等），内存随对话量线性增长。

修复方式（不改动 AstrBot 本体，纯插件层 patch）:
  1. 包装 register：注册时把当时的 umo 存进 event extras；
  2. 包装 unregister：优先用注册时保存的 key 做 discard。
  terminate 时完整还原，不影响插件卸载/重载。
"""

from astrbot.api import logger, star
from astrbot.api.star import register

_REGISTRY_KEY_EXTRA = "_active_event_registry_key"
_PATCHED_FLAG = "_event_registry_fix_patched"


@register(
    "astrbot_event_registry_fix",
    "workbuddy",
    "修复 unique_session 会话隔离导致的 active_event_registry 事件泄漏",
    "1.1.0",
)
class EventRegistryFix(star.Star):
    def __init__(self, context: star.Context) -> None:
        super().__init__(context)
        self._registry = None
        self._orig_register = None
        self._orig_unregister = None
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

        self._apply_patch()
        logger.info("[EventRegistryFix] 已挂载事件注册表泄漏修复。")

    # ── patch ──────────────────────────────────────────────

    def _apply_patch(self) -> None:
        reg = self._registry
        if getattr(reg, _PATCHED_FLAG, False):
            return  # 已打过（如 initialize 被重复调用）

        self._orig_register = reg.register
        self._orig_unregister = reg.unregister

        def patched_register(event, _orig=self._orig_register):
            try:
                event.set_extra(_REGISTRY_KEY_EXTRA, event.unified_msg_origin)
            except Exception:  # noqa: BLE001
                pass
            return _orig(event)

        def patched_unregister(event, _orig=self._orig_unregister):
            try:
                umo = event.get_extra(_REGISTRY_KEY_EXTRA) or event.unified_msg_origin
                reg._agent_stop_callbacks.pop(event, None)
                events_set = reg._events.get(umo)
                if events_set is not None:
                    events_set.discard(event)
                    if not events_set:
                        reg._events.pop(umo, None)
                return
            except Exception:  # noqa: BLE001
                # 兜底走原实现，保证注销动作绝不缺失
                return _orig(event)

        # 用实例属性覆盖类方法（仅影响单例实例，可完整还原）
        reg.register = patched_register
        reg.unregister = patched_unregister
        setattr(reg, _PATCHED_FLAG, True)
        self._patched = True

    def _remove_patch(self) -> None:
        reg = self._registry
        if reg is None or not self._patched:
            return
        reg.register = self._orig_register
        reg.unregister = self._orig_unregister
        setattr(reg, _PATCHED_FLAG, False)
        self._patched = False

    async def terminate(self) -> None:
        self._remove_patch()
        logger.info("[EventRegistryFix] 已还原事件注册表。")
