"""降采样复查与质量处置流程测试：证明复查不重算、证据不被覆盖、派补取处置单判定。"""
from __future__ import annotations

import pytest

from app.services.downsample import DownsampleService
from app.store import store


@pytest.fixture
def downsample():
    return DownsampleService()


@pytest.fixture(autouse=True)
def reset_downsample_state():
    """每个用例都从 R1 原始证据开始，避免动作测试污染后续断言。"""
    store._aux["downsample_rounds"] = store._aux["downsample_rounds"][:1]
    store._aux["downsample_reviews"] = []
    store._aux["downsample_dispositions"] = []
    task = store.find("downsample", 1)
    task["完成时间"] = "2026-09-01T00:12:00+08:00"
    task["status"] = "已完成"
    task["执行状态"] = "已完成"
    task["abnormal"] = True
    yield


def _task_completion() -> str:
    return store.find("downsample", 1)["完成时间"]


def test_plain_review_does_not_recompute_or_rewrite_completion(downsample):
    before_rounds = len(downsample._rounds_for("DOWN-0001"))
    before_completion = _task_completion()

    opened = downsample.open_review("DOWN-0001")
    assert opened["ok"] is True
    assert opened["review"]["result_generated"] is False
    assert opened["review"]["round_id"] == "R1"

    # 单纯复查：不新增轮次，不改任务完成时间，不生成结果。
    assert len(downsample._rounds_for("DOWN-0001")) == before_rounds
    assert _task_completion() == before_completion
    assert downsample._find_review(opened["review"]["review_id"])["result_generated"] is False


def test_review_evidence_comes_from_referenced_round(downsample):
    opened = downsample.open_review("DOWN-0001")
    review = downsample.get_review(opened["review"]["review_id"])["review"]
    assert review["mean"] == 20.0
    assert review["anomaly_label"] == "甲窗"
    assert review["completion_time"] == "2026-09-01T00:12:00+08:00"

    # 再次打开（浏览器刷新）应取得同一份证据。
    again = downsample.get_review(opened["review"]["review_id"])["review"]
    assert again["mean"] == 20.0
    assert again["completion_time"] == review["completion_time"]
    assert _task_completion() == "2026-09-01T00:12:00+08:00"


def test_disposition_uses_single_round_facts(downsample):
    opened = downsample.open_review("DOWN-0001")
    generated = downsample.generate_disposition(opened["review"]["review_id"])
    assert generated["ok"] is True
    disposition = generated["disposition"]

    assert disposition["round_id"] == "R1"
    assert disposition["mean"] == 20.0
    assert disposition["completion_time"] == "2026-09-01T00:12:00+08:00"
    assert disposition["anomaly_label"] == "甲窗"
    assert disposition["anomaly_window"]["window_start"] == "2026-09-01T00:00:00+08:00"


def test_dispatch_window_comes_from_disposition_not_latest_round(downsample):
    opened = downsample.open_review("DOWN-0001")
    disposition = downsample.generate_disposition(opened["review"]["review_id"])["disposition"]

    # 另一个窗口明确重算：新轮次异常在乙窗。
    recomputed = downsample.execute_downsample("DOWN-0001", window_values=[30.0, 10.0])
    assert recomputed["round_id"] == "R2"
    assert recomputed["round"]["anomaly_label"] == "乙窗"

    dispatched = downsample.dispatch_disposition(disposition["disposition_id"])
    assert dispatched["ok"] is True
    gap = dispatched["gap"]
    assert gap["异常窗标签"] == "甲窗"
    assert gap["中断起始"] == "2026-09-01T00:00:00+08:00"
    assert gap["中断结束"] == "2026-09-01T00:05:00+08:00"
    assert gap["依据轮次"] == "R1"


def test_old_review_detects_superseded_round_but_keeps_old_facts(downsample):
    opened = downsample.open_review("DOWN-0001")
    review_id = opened["review"]["review_id"]
    generated = downsample.generate_disposition(review_id)["disposition"]

    downsample.execute_downsample("DOWN-0001", window_values=[10.0, 30.0])

    continuity = downsample.check_review_continuity(review_id)
    assert continuity["can_continue"] is False
    assert continuity["continuity"] == "superseded"
    assert continuity["superseded_by"] == "R2"
    assert continuity["round_id"] == "R1"

    # 旧复查仍以 R1 事实回答，不能把 R2 的乙窗当成 R1 的异常。
    review = downsample.get_review(review_id)["review"]
    assert review["mean"] == 20.0
    assert review["anomaly_label"] == "甲窗"
    assert review["superseded_by"] == "R2"

    blocked = downsample.generate_disposition(review_id, require_current=True)
    assert blocked["ok"] is False
    assert blocked["can_continue"] is False

    # 已生成处置单的判定不因 R2 改变。
    stored = downsample.get_disposition(generated["disposition_id"])["disposition"]
    assert stored["mean"] == 20.0
    assert stored["anomaly_label"] == "甲窗"
    assert stored["basis_still_current"] is False


def test_history_round_mean_survives_recompute(downsample):
    before = downsample.quality_check("DOWN-0001")
    downsample.execute_downsample("DOWN-0001")
    after_rounds = downsample.task_detail("DOWN-0001")["rounds"]
    assert after_rounds[0]["round_id"] == "R1"
    assert after_rounds[0]["mean"] == 20.0
    assert before["mean"] == 20.0


def test_reopen_review_flag_does_not_change_completion_record(downsample):
    opened = downsample.open_review("DOWN-0001")
    completion = _task_completion()
    downsample.get_review(opened["review"]["review_id"], mark_reopened=True)
    assert _task_completion() == completion
    assert len(downsample._rounds_for("DOWN-0001")) == 1
