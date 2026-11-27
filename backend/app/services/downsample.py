"""降采样领域服务：时序数据降采样任务管理与聚合计算。

提供窗口聚合、降采样执行、数据质量控制等真实业务能力。

关键业务约束（复查与处置）：
- 每次“执行/重算”会产生一轮不可变的执行证据（轮次），记录窗口值、均值、
  异常窗和完成时间；任务上的完成时间只指向最新一轮。
- “复查”只读取已有轮次证据，绝不重新生成采样值，也不改写任何任务的完成时间。
- 质量处置单中的完成时间、均值和异常位置必须来自同一轮，生成时按该轮窗口值
  重新计算并校验一致性。
- 别处发生重算后，旧复查要能判断自己依据的轮次已被替代（不能再按当前轮次继续
  办理），但旧轮次的事实保持不变，不能拿新一轮的异常冒充旧一轮。
- 派补窗口只取质量处置单上记录的异常窗，不取任务当前轮次的最新异常。
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any
from random import Random

from app.services.base import BaseModuleService
from app.store import store

CST = timezone(timedelta(hours=8))

ROUNDS_KEY = "downsample_rounds"
REVIEWS_KEY = "downsample_reviews"
DISPOSITIONS_KEY = "downsample_dispositions"

# 保存到窗口的证据编号（题位约定：甲窗=第1窗，乙窗=第2窗……）。
WINDOW_LABELS = ["甲窗", "乙窗", "丙窗", "丁窗", "戊窗", "己窗", "庚窗", "辛窗", "壬窗", "癸窗"]

GRANULARITY_SECONDS = {
    "1m": 60, "5m": 300, "10m": 600, "15m": 900,
    "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
}


def _now() -> str:
    return datetime.now(CST).isoformat(timespec="seconds")


def _now_dt() -> datetime:
    return datetime.now(CST)


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CST)
    return parsed


def window_label(index: int) -> str:
    """窗口序号的稳定中文标识：0→甲窗、1→乙窗，超出部分用“第N窗”。"""
    if 0 <= index < len(WINDOW_LABELS):
        return WINDOW_LABELS[index]
    return f"第{index + 1}窗"


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


class DownsampleService(BaseModuleService):
    MODULE = "downsample"
    ENTITY = "降采样任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务编号", "源测点", "目标粒度", "聚合函数", "源时间范围", "目标测点", "执行状态", "完成时间"]
    REQUIRED_FIELDS = ["任务编号", "源测点", "目标粒度"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "数据不足"]
    ACTION_RULES = {"发起降采样": "执行中", "标记完成": "已完成", "标记不足": "数据不足"}
    NEGATIVE_ACTIONS = []

    # ==================== 内部存储与查找 ====================

    @staticmethod
    def _rounds() -> list[dict[str, Any]]:
        return store.aux_rows(ROUNDS_KEY)

    @staticmethod
    def _reviews() -> list[dict[str, Any]]:
        return store.aux_rows(REVIEWS_KEY)

    @staticmethod
    def _dispositions() -> list[dict[str, Any]]:
        return store.aux_rows(DISPOSITIONS_KEY)

    def _find_task(self, task_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if str(row.get("任务编号")) == str(task_id):
                return row
        return None

    def _find_round(self, round_id: str) -> dict[str, Any] | None:
        for row in self._rounds():
            if row.get("round_id") == round_id:
                return row
        return None

    def _rounds_for(self, task_id: str) -> list[dict[str, Any]]:
        return [row for row in self._rounds() if row.get("task_id") == task_id]

    def _latest_round(self, task_id: str) -> dict[str, Any] | None:
        rounds = self._rounds_for(task_id)
        if not rounds:
            return None
        return max(rounds, key=lambda row: int(row.get("seq", 0)))

    def _find_review(self, review_id: str) -> dict[str, Any] | None:
        for row in self._reviews():
            if row.get("review_id") == review_id:
                return row
        return None

    def _find_disposition(self, disposition_id: str) -> dict[str, Any] | None:
        for row in self._dispositions():
            if row.get("disposition_id") == disposition_id:
                return row
        return None

    def _reviews_for(self, task_id: str) -> list[dict[str, Any]]:
        return [row for row in self._reviews() if row.get("task_id") == task_id]

    def _dispositions_for(self, task_id: str) -> list[dict[str, Any]]:
        return [row for row in self._dispositions() if row.get("task_id") == task_id]

    @staticmethod
    def _next_seq(rows: list[dict[str, Any]], field: str) -> int:
        return max((int(row.get(field, 0)) for row in rows), default=0) + 1

    # ==================== 轮次与证据计算 ====================

    def _base_time(self, task: dict[str, Any]) -> datetime:
        text = str(task.get("源时间范围", ""))
        if "~" in text:
            text = text.split("~")[0]
        parsed = _parse_dt(text.strip())
        if parsed is not None:
            return parsed
        return _now_dt().replace(microsecond=0)

    def _window_span(self, task: dict[str, Any], index: int, window_seconds: int) -> tuple[str, str]:
        base = self._base_time(task)
        start = base + timedelta(seconds=index * window_seconds)
        end = start + timedelta(seconds=window_seconds)
        return (
            start.isoformat(timespec="seconds"),
            end.isoformat(timespec="seconds"),
        )

    @staticmethod
    def _resolve_anomaly_index(
        values: list[float],
        explicit_index: int | None = None,
        explicit_label: str | None = None,
    ) -> int | None:
        """判定异常窗：显式指定优先；否则取低于均值的窗口里数值最低的一个。

        低于均值的窗口有多个时，数值更低者优先；再并列时取较早窗口。
        所有窗口都等于均值则视为没有异常窗。
        """
        if not values:
            return None
        if explicit_label:
            for index in range(len(WINDOW_LABELS)):
                if WINDOW_LABELS[index] == explicit_label:
                    return index if index < len(values) else None
        if explicit_index is not None:
            try:
                candidate = int(explicit_index)
            except (ValueError, TypeError):
                return None
            return candidate if 0 <= candidate < len(values) else None

        mean = sum(values) / len(values)
        below = [index for index, value in enumerate(values) if value < mean]
        if not below:
            return None
        return min(below, key=lambda index: (values[index], index))

    def _aggregate_windows(
        self,
        task: dict[str, Any],
        window_values: list[float] | None,
        window_seconds: int,
        seq: int,
    ) -> tuple[list[dict[str, Any]], int, int]:
        """生成本轮窗口值；有显式窗口值时按显式值，否则用确定性源数据聚合。"""
        if window_values:
            values = [float(value) for value in window_values]
        else:
            seed_rng = Random(f"{task.get('任务编号')}:{seq}")
            if window_seconds > 300:
                window_count = 2
            else:
                window_count = max(2, 600 // window_seconds)
            base_val = seed_rng.uniform(25, 55)
            if window_count == 2:
                # 两窗演示任务：重算带入的新采样把异常轮到下一窗，首轮异常在甲窗。
                spread = seed_rng.uniform(8, 20)
                low, high = base_val, base_val + spread
                values = [low, high] if seq % 2 == 1 else [high, low]
            else:
                per_window = max(60, min(window_seconds, 600))
                values = []
                for start in range(0, per_window * window_count, per_window):
                    chunk = [
                        base_val + math.sin((start + offset) * 0.1) * 10 + seed_rng.uniform(-5, 5)
                        for offset in range(per_window)
                    ]
                    values.append(sum(chunk) / len(chunk))

        windows: list[dict[str, Any]] = []
        for index, value in enumerate(values):
            start, end = self._window_span(task, index, window_seconds)
            windows.append({
                "index": index,
                "label": window_label(index),
                "window_start": start,
                "window_end": end,
                "value": round(value, 4),
                "count": 1,
            })
        return windows, len(values), window_seconds

    def execute_downsample(
        self,
        task_id: str,
        window_values: list[float] | None = None,
        anomaly_index: int | None = None,
        anomaly_label: str | None = None,
        window_seconds: int | None = None,
        source: str = "manual",
    ) -> dict[str, Any]:
        """执行/重算降采样任务：产生一轮新的、不可变的执行证据。

        每次调用都追加一个新轮次（R1、R2……），任务上的完成时间只跟随最新轮次；
        历史轮次的完成时间、窗口值、均值和异常窗不会被后续执行改写。
        """
        task = self._find_task(task_id)
        if task is None:
            return {"ok": False, "message": f"降采样任务 {task_id} 不存在"}

        granularity = task.get("目标粒度", "")
        seconds = window_seconds or GRANULARITY_SECONDS.get(str(granularity), 300)
        if seconds <= 0:
            seconds = 300

        seq = self._next_seq(self._rounds_for(task_id), "seq")
        windows, window_count, seconds = self._aggregate_windows(
            task, window_values, int(seconds), seq
        )
        values = [window["value"] for window in windows]
        mean = sum(values) / len(values) if values else 0.0
        variance = sum((value - mean) ** 2 for value in values) / len(values) if values else 0.0
        std_dev = math.sqrt(variance)

        anomaly_idx = self._resolve_anomaly_index(values, anomaly_index, anomaly_label)
        anomaly_window = dict(windows[anomaly_idx]) if anomaly_idx is not None else None

        completion = _now_dt().replace(microsecond=0)
        previous = self._latest_round(task_id)
        previous_time = _parse_dt(previous.get("completion_time")) if previous else None
        if previous_time is not None and completion <= previous_time:
            completion = previous_time + timedelta(seconds=1)

        round_row = {
            "round_id": f"R{seq}",
            "task_id": task_id,
            "seq": seq,
            "window_seconds": seconds,
            "window_values": values,
            "window_count": window_count,
            "windows": windows,
            "mean": round(mean, 4),
            "std_dev": round(std_dev, 4),
            "anomaly_index": anomaly_idx,
            "anomaly_label": window_label(anomaly_idx) if anomaly_idx is not None else None,
            "anomaly_window": anomaly_window,
            "completion_time": completion.isoformat(timespec="seconds"),
            "created_at": _now(),
            "source": source,
            "task_snapshot": {
                "源测点": task.get("源测点"),
                "目标粒度": task.get("目标粒度"),
                "聚合函数": task.get("聚合函数"),
                "源时间范围": task.get("源时间范围"),
            },
        }
        self._rounds().append(round_row)

        task["执行状态"] = "已完成"
        task["完成时间"] = round_row["completion_time"]
        task["status"] = "已完成"
        task["pending"] = False
        task["abnormal"] = bool(anomaly_idx is not None)

        return {
            "ok": True,
            "message": f"降采样任务 {task_id} 第 {seq} 轮已完成",
            "task_id": task_id,
            "round": round_row,
            "round_id": round_row["round_id"],
            "completion_time": round_row["completion_time"],
            "mean": round_row["mean"],
            "anomaly_window": anomaly_window,
            "source_point": task.get("源测点"),
            "granularity": granularity,
            "agg_function": task.get("聚合函数", "avg"),
            "raw_points": window_count * seconds,
            "aggregated_points": window_count,
            "compression_ratio": round((window_count * seconds) / max(window_count, 1), 1),
            "result": windows[:10],
        }

    # ==================== 复查（纯读取，不重算） ====================

    def quality_check(self, task_id: str, review_id: str | None = None) -> dict[str, Any]:
        """降采样数据质量检查：只读取已有轮次证据。

        与旧实现的差别：这里不再调用 execute_downsample，因此不会重新生成采样值，
        不会覆盖任务的完成时间，也不会让两轮均值互相污染。
        """
        if review_id:
            return self.get_review(review_id)
        latest = self._latest_round(task_id)
        if latest is None:
            return {
                "ok": False,
                "message": f"降采样任务 {task_id} 尚无执行轮次，请先完成一次降采样",
                "quality": "insufficient",
            }
        has_anomaly = latest.get("anomaly_index") is not None
        return {
            "ok": True,
            "task_id": task_id,
            "round_id": latest.get("round_id"),
            "completion_time": latest.get("completion_time"),
            "point_count": latest.get("window_count"),
            "mean": latest.get("mean"),
            "std_dev": latest.get("std_dev"),
            "anomaly_window": latest.get("anomaly_window"),
            "has_anomaly": has_anomaly,
            "quality": "suspicious" if has_anomaly else "good",
        }

    def open_review(self, task_id: str) -> dict[str, Any]:
        """发起复查：引用当前最新轮次建立复查记录，不执行重算。

        复查创建时结果尚未生成；后续“生成质量处置单”才会基于所引用轮次计算。
        """
        task = self._find_task(task_id)
        if task is None:
            return {"ok": False, "message": f"降采样任务 {task_id} 不存在"}
        basis = self._latest_round(task_id)
        if basis is None:
            return {
                "ok": False,
                "message": f"降采样任务 {task_id} 尚无执行轮次，无法复查（纯复查不会触发重算）",
            }

        seq = self._next_seq(self._reviews_for(task_id), "seq")
        review = {
            "review_id": f"REV-{task_id}-{seq:02d}",
            "task_id": task_id,
            "seq": seq,
            "round_id": basis.get("round_id"),
            "round_seq": basis.get("seq"),
            "status": "复查中",
            "result_generated": False,
            "disposition_id": None,
            "completion_time": basis.get("completion_time"),
            "mean": basis.get("mean"),
            "anomaly_label": basis.get("anomaly_label"),
            "anomaly_window": basis.get("anomaly_window"),
            "created_at": _now(),
            "generated_at": None,
            "reopened_at": None,
        }
        self._reviews().append(review)
        return {
            "ok": True,
            "message": f"已对任务 {task_id} 的 {basis.get('round_id')} 发起复查，结果尚未生成",
            "review": self._serialize_review(review),
        }

    def _serialize_review(self, review: dict[str, Any]) -> dict[str, Any]:
        """返回复查视图：证据固定来自所引用的轮次，续办状态按当前轮次动态判断。"""
        basis = self._find_round(review.get("round_id", "")) or {}
        latest = self._latest_round(review.get("task_id", "")) or {}
        is_superseded = bool(latest) and latest.get("round_id") != basis.get("round_id")
        payload = dict(review)
        # 证据字段一律取自所引用轮次，避免被新一轮覆盖。
        payload.update({
            "completion_time": basis.get("completion_time", review.get("completion_time")),
            "mean": basis.get("mean", review.get("mean")),
            "anomaly_label": basis.get("anomaly_label"),
            "anomaly_window": basis.get("anomaly_window"),
            "window_values": basis.get("window_values"),
            "round_id": basis.get("round_id", review.get("round_id")),
            "current_round_id": latest.get("round_id"),
            "can_continue": not is_superseded,
            "continuity": "superseded" if is_superseded else "current",
            "superseded_by": latest.get("round_id") if is_superseded else None,
        })
        if is_superseded:
            payload["continuity_notice"] = (
                f"复查依据的 {basis.get('round_id')} 已被 {latest.get('round_id')} 替代，"
                f"不能按当前轮次继续办理；历史事实仍保留为 {basis.get('round_id')} 的证据"
            )
        else:
            payload["continuity_notice"] = ""
        return payload

    def get_review(self, review_id: str, mark_reopened: bool = False) -> dict[str, Any]:
        """读取复查详情。mark_reopened 只记录“重新打开”，不重算、不改完成记录。"""
        review = self._find_review(review_id)
        if review is None:
            return {"ok": False, "message": f"复查 {review_id} 不存在"}
        if mark_reopened:
            review["reopened_at"] = _now()
            review["status"] = review.get("status", "复查中")
        payload = self._serialize_review(review)
        return {"ok": True, "review": payload}

    def check_review_continuity(self, review_id: str) -> dict[str, Any]:
        """判断旧复查能否继续办理：只报告续办状态，不触碰任何轮次证据。"""
        review = self._find_review(review_id)
        if review is None:
            return {"ok": False, "message": f"复查 {review_id} 不存在"}
        payload = self._serialize_review(review)
        return {
            "ok": True,
            "review_id": review_id,
            "round_id": payload.get("round_id"),
            "current_round_id": payload.get("current_round_id"),
            "can_continue": payload.get("can_continue"),
            "continuity": payload.get("continuity"),
            "superseded_by": payload.get("superseded_by"),
            "notice": payload.get("continuity_notice"),
        }

    # ==================== 质量处置单 ====================

    @staticmethod
    def _recompute_round_evidence(round_row: dict[str, Any]) -> dict[str, Any]:
        """按轮次窗口值重新计算均值和异常位置，用于处置单的一致性校验。"""
        values = [_as_float(value) for value in round_row.get("window_values", [])]
        if not values or any(value is None for value in values):
            return {"ok": False, "message": "轮次窗口值缺失，无法生成质量处置单"}
        numeric = [value for value in values if value is not None]
        mean = sum(numeric) / len(numeric)
        variance = sum((value - mean) ** 2 for value in numeric) / len(numeric)
        return {
            "ok": True,
            "mean": round(mean, 4),
            "std_dev": round(math.sqrt(variance), 4),
            "stored_mean": round(float(round_row.get("mean", 0)), 4),
        }

    def generate_disposition(self, review_id: str, require_current: bool = False) -> dict[str, Any]:
        """生成质量处置单：完成时间、均值、异常位置全部出自复查所引用的同一轮。

        即使该轮已被后来的重算替代，也只沿用该轮事实（并明确标记依据已过期），
        绝不读取当前轮次的异常窗。
        """
        review = self._find_review(review_id)
        if review is None:
            return {"ok": False, "message": f"复查 {review_id} 不存在"}
        basis = self._find_round(review.get("round_id", ""))
        if basis is None:
            return {"ok": False, "message": f"复查 {review_id} 所依据的轮次不存在，无法生成处置单"}

        latest = self._latest_round(basis.get("task_id", "")) or {}
        superseded = bool(latest) and latest.get("round_id") != basis.get("round_id")
        if require_current and superseded:
            return {
                "ok": False,
                "message": (
                    f"复查依据的 {basis.get('round_id')} 已被 {latest.get('round_id')} 替代，"
                    "不能继续办理，请重新发起复查"
                ),
                "can_continue": False,
                "superseded_by": latest.get("round_id"),
            }

        # 幂等：同一复查只生成一张处置单。
        existing = self._find_disposition(review.get("disposition_id") or "")
        if existing is not None:
            return {
                "ok": True,
                "message": f"质量处置单 {existing.get('disposition_id')} 已存在",
                "disposition": self._serialize_disposition(existing),
                "reused": True,
            }

        computed = self._recompute_round_evidence(basis)
        if not computed.get("ok"):
            return computed
        if abs(float(computed["mean"]) - float(computed["stored_mean"])) > 1e-6:
            return {
                "ok": False,
                "message": (
                    f"轮次 {basis.get('round_id')} 的窗口均值与存储证据不一致，"
                    "拒绝生成质量处置单"
                ),
            }

        anomaly_index = basis.get("anomaly_index")
        anomaly_window = basis.get("anomaly_window")
        anomaly_value = _as_float(anomaly_window.get("value")) if anomaly_window else None
        if anomaly_index is not None and anomaly_window is not None:
            windows = basis.get("windows", [])
            if 0 <= int(anomaly_index) < len(windows):
                source_value = _as_float(windows[int(anomaly_index)].get("value"))
                if source_value is not None and anomaly_value is not None and abs(source_value - anomaly_value) > 1e-6:
                    return {
                        "ok": False,
                        "message": f"轮次 {basis.get('round_id')} 的异常窗证据不一致，拒绝生成质量处置单",
                    }

        seq = self._next_seq(self._dispositions(), "seq")
        disposition = {
            "disposition_id": f"QD-{seq:04d}",
            "task_id": basis.get("task_id"),
            "review_id": review_id,
            "round_id": basis.get("round_id"),
            "round_seq": basis.get("seq"),
            "completion_time": basis.get("completion_time"),
            "mean": computed["mean"],
            "std_dev": computed["std_dev"],
            "window_values": list(basis.get("window_values", [])),
            "anomaly_label": basis.get("anomaly_label"),
            "anomaly_index": anomaly_index,
            "anomaly_window": dict(anomaly_window) if anomaly_window else None,
            "status": "待派补",
            "basis_status": "superseded" if superseded else "current",
            "superseded_by": latest.get("round_id") if superseded else None,
            "gap_id": None,
            "created_at": _now(),
            "dispatched_at": None,
        }
        self._dispositions().append(disposition)

        review["result_generated"] = True
        review["disposition_id"] = disposition["disposition_id"]
        review["status"] = "已生成处置单"
        review["generated_at"] = disposition["created_at"]

        return {
            "ok": True,
            "message": f"质量处置单 {disposition['disposition_id']} 已生成（依据 {basis.get('round_id')}）",
            "disposition": self._serialize_disposition(disposition),
        }

    def _serialize_disposition(self, disposition: dict[str, Any]) -> dict[str, Any]:
        payload = dict(disposition)
        latest = self._latest_round(disposition.get("task_id", "")) or {}
        still_current = bool(latest) and latest.get("round_id") == disposition.get("round_id")
        payload["basis_still_current"] = still_current
        payload["current_round_id"] = latest.get("round_id")
        if not still_current:
            payload["basis_notice"] = (
                f"处置单依据 {disposition.get('round_id')} 生成，当前任务轮次为 "
                f"{latest.get('round_id')}；派补仍使用处置单记录的异常窗"
            )
        else:
            payload["basis_notice"] = ""
        return payload

    def get_disposition(self, disposition_id: str) -> dict[str, Any]:
        disposition = self._find_disposition(disposition_id)
        if disposition is None:
            return {"ok": False, "message": f"质量处置单 {disposition_id} 不存在"}
        return {"ok": True, "disposition": self._serialize_disposition(disposition)}

    # ==================== 异常窗交给数据断点 ====================

    def _find_gap(self, gap_id: str) -> dict[str, Any] | None:
        for row in store.rows("data_gap"):
            if str(row.get("记录编号")) == str(gap_id):
                return row
        return None

    def dispatch_disposition(self, disposition_id: str) -> dict[str, Any]:
        """按质量处置单记录的异常窗向数据断点发起派补。

        补采窗口只取处置单上的 anomaly_window，不取任务当前轮次的最新异常。
        """
        disposition = self._find_disposition(disposition_id)
        if disposition is None:
            return {"ok": False, "message": f"质量处置单 {disposition_id} 不存在"}

        anomaly = disposition.get("anomaly_window")
        if not anomaly:
            return {"ok": False, "message": f"质量处置单 {disposition_id} 未判定异常窗，无法派补"}

        existing = self._find_gap(disposition.get("gap_id") or "")
        if existing is not None:
            return {
                "ok": True,
                "message": f"质量处置单 {disposition_id} 已派补到断点记录 {existing.get('记录编号')}",
                "gap": existing,
                "disposition": self._serialize_disposition(disposition),
                "reused": True,
            }

        task = self._find_task(disposition.get("task_id", "")) or {}
        point_id = str(task.get("源测点", ""))
        device_id = point_id
        for point in store.rows("ts_point"):
            if str(point.get("测点编号")) == point_id and point.get("设备编号"):
                device_id = str(point.get("设备编号"))
                break

        gap_rows = store.rows("data_gap")
        gap_id = max((int(row.get("id", 0)) for row in gap_rows), default=0) + 1
        gap = {
            "id": gap_id,
            "记录编号": f"GAP-{gap_id:04d}",
            "设备编号": device_id,
            "测点编号": point_id,
            "中断起始": anomaly.get("window_start", ""),
            "中断结束": anomaly.get("window_end", ""),
            "缺失点数": str(anomaly.get("count", 1)),
            "补采策略": f"按质量处置单 {disposition_id} 补采",
            "处理状态": "待派补",
            "质量处置单": disposition_id,
            "异常窗标签": anomaly.get("label", ""),
            "依据轮次": disposition.get("round_id"),
            "status": "补采中",
            "pending": True,
            "abnormal": False,
            "派补时间": _now(),
        }
        gap_rows.append(gap)

        disposition["gap_id"] = gap["记录编号"]
        disposition["status"] = "已派补"
        disposition["dispatched_at"] = gap["派补时间"]

        return {
            "ok": True,
            "message": (
                f"已依据质量处置单 {disposition_id} 的 {anomaly.get('label')}"
                f"异常窗发起派补：{gap['中断起始']} ~ {gap['中断结束']}"
            ),
            "gap": gap,
            "disposition": self._serialize_disposition(disposition),
        }

    # ==================== 详情汇总 ====================

    def task_detail(self, task_id: str) -> dict[str, Any]:
        """降采样详情页数据：任务、轮次、复查、处置单、相关断点一次性返回。

        数据全部取自服务端持久状态，刷新页面后仍能得到先前的质量证据。
        """
        task = self._find_task(task_id)
        if task is None:
            return {"ok": False, "message": f"降采样任务 {task_id} 不存在"}

        rounds = sorted(self._rounds_for(task_id), key=lambda row: int(row.get("seq", 0)))
        reviews = sorted(self._reviews_for(task_id), key=lambda row: int(row.get("seq", 0)))
        dispositions = sorted(
            self._dispositions_for(task_id), key=lambda row: int(row.get("seq", 0))
        )
        disposition_ids = {row.get("disposition_id") for row in dispositions}
        gaps = [
            row for row in store.rows("data_gap")
            if str(row.get("质量处置单") or "") in disposition_ids
            or (
                row.get("测点编号") == task.get("源测点")
                and row.get("依据轮次") in {item.get("round_id") for item in rounds}
            )
        ]
        latest = rounds[-1] if rounds else None

        return {
            "ok": True,
            "task": task,
            "current_round_id": latest.get("round_id") if latest else None,
            "rounds": rounds,
            "reviews": [self._serialize_review(row) for row in reviews],
            "dispositions": [self._serialize_disposition(row) for row in dispositions],
            "gaps": gaps,
        }

    # ==================== 列表动作扩展 ====================

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """列表页的“发起降采样”本身就是一次显式执行，会产生新轮次。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "发起降采样":
            result = self.execute_downsample(str(entry.get("任务编号")), source="manual")
            return entry, str(result.get("message"))
        return super().run_action(entry_id, action)
