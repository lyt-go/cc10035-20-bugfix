"""降采样复查与质量处置接口：执行轮次、复查、处置单与断点派补。

所有读取都返回服务端持久证据，页面刷新或重新打开浏览器后仍可取得同一轮结果。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services import downsample_service

router = APIRouter(prefix="/api/downsample", tags=["降采样复查"])
service = downsample_service



class ExecutePayload(BaseModel):
    window_values: list[float] | None = Field(default=None, description="显式窗口值")
    anomaly_index: int | None = Field(default=None, description="显式异常窗序号（0 起）")
    anomaly_label: str | None = Field(default=None, description="显式异常窗名称，如甲窗")
    window_seconds: int | None = Field(default=None, description="窗口秒数")


class DispositionPayload(BaseModel):
    require_current: bool = Field(default=False, description="是否要求复查依据的轮次仍为当前轮")


@router.get("/{task_id}/detail")
def task_detail(task_id: str) -> dict[str, Any]:
    """降采样详情：任务、执行轮次、复查、处置单与派补断点。"""
    return service.task_detail(task_id)


@router.post("/{task_id}/reviews")
def open_review(task_id: str) -> dict[str, Any]:
    """发起复查：引用当前轮次建立复查，不在此时重算。"""
    return service.open_review(task_id)


@router.get("/reviews/{review_id}")
def get_review(review_id: str, reopen: bool = Query(default=False)) -> dict[str, Any]:
    """读取复查；reopen=true 只记录重新打开，不改完成记录也不重算。"""
    return service.get_review(review_id, mark_reopened=reopen)


@router.get("/reviews/{review_id}/continuity")
def review_continuity(review_id: str) -> dict[str, Any]:
    """判断旧复查能否继续办理，以及是否已被新一轮替代。"""
    return service.check_review_continuity(review_id)


@router.post("/reviews/{review_id}/disposition")
def generate_disposition(
    review_id: str, payload: DispositionPayload | None = None,
) -> dict[str, Any]:
    """按复查所引用轮次的真实计算生成质量处置单。"""
    require_current = bool(payload and payload.require_current)
    return service.generate_disposition(review_id, require_current=require_current)


@router.get("/dispositions/{disposition_id}")
def get_disposition(disposition_id: str) -> dict[str, Any]:
    return service.get_disposition(disposition_id)


@router.post("/dispositions/{disposition_id}/dispatch")
def dispatch_disposition(disposition_id: str) -> dict[str, Any]:
    """依据处置单记录的异常窗向数据断点派补。"""
    return service.dispatch_disposition(disposition_id)


@router.post("/{task_id}/execute")
def execute_downsample(
    task_id: str, payload: ExecutePayload | None = None,
) -> dict[str, Any]:
    """显式执行/重算：新增一轮证据，并让任务完成时间跟随新轮次。"""
    body = payload or ExecutePayload()
    return service.execute_downsample(
        task_id,
        window_values=body.window_values,
        anomaly_index=body.anomaly_index,
        anomaly_label=body.anomaly_label,
        window_seconds=body.window_seconds,
        source="manual",
    )
