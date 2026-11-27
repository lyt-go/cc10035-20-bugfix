"""跨模块数据流综合测试：覆盖18个领域服务和10组跨模块数据流。

用种子数据验证每个Service的核心方法和跨模块集成。
"""
from __future__ import annotations

import pytest

from app.store import store
from app.services.device_registry import DeviceRegistryService
from app.services.protocol_cfg import ProtocolCfgService
from app.services.msg_decode import MsgDecodeService
from app.services.msg_router import MsgRouterService
from app.services.ts_point import TsPointService
from app.services.ts_store import TsStoreService
from app.services.downsample import DownsampleService
from app.services.alarm_trigger import AlarmTriggerService
from app.services.rule_engine import RuleEngineService
from app.services.shadow_dev import ShadowDevService
from app.services.tag_binding import TagBindingService
from app.services.gateway_mgmt import GatewayMgmtService
from app.services.edge_cache import EdgeCacheService
from app.services.firmware_ota import FirmwareOtaService
from app.services.aggregate_sink import AggregateSinkService
from app.services.data_stream import DataStreamService
from app.services.batch_export import BatchExportService
from app.services.cross_module import cross_service


# ========================
# Fixtures
# ========================

@pytest.fixture
def device_registry():
    return DeviceRegistryService()


@pytest.fixture
def protocol_cfg():
    return ProtocolCfgService()


@pytest.fixture
def msg_decode():
    return MsgDecodeService()


@pytest.fixture
def msg_router():
    return MsgRouterService()


@pytest.fixture
def ts_point():
    return TsPointService()


@pytest.fixture
def ts_store():
    return TsStoreService()


@pytest.fixture
def downsample():
    return DownsampleService()


@pytest.fixture
def alarm_trigger():
    return AlarmTriggerService()


@pytest.fixture
def rule_engine():
    return RuleEngineService()


@pytest.fixture
def shadow_dev():
    return ShadowDevService()


@pytest.fixture
def tag_binding():
    return TagBindingService()


@pytest.fixture
def gateway_mgmt():
    return GatewayMgmtService()


@pytest.fixture
def edge_cache():
    return EdgeCacheService()


@pytest.fixture
def firmware_ota():
    return FirmwareOtaService()


@pytest.fixture
def aggregate_sink():
    return AggregateSinkService()


@pytest.fixture
def data_stream():
    return DataStreamService()


@pytest.fixture
def batch_export():
    return BatchExportService()


# ========================
# 基础Service方法测试
# ========================

class TestDeviceRegistry:
    def test_heartbeat_existing_device(self, device_registry):
        result = device_registry.heartbeat("DEVI-0001")
        assert result["ok"] is True

    def test_heartbeat_nonexistent_device(self, device_registry):
        result = device_registry.heartbeat("NONEXIST")
        assert result["ok"] is False

    def test_check_offline_devices(self, device_registry):
        result = device_registry.check_offline_devices()
        assert isinstance(result, list)

    def test_match_protocol(self, device_registry):
        result = device_registry.match_protocol("设备注册样例1")
        assert isinstance(result, list)

    def test_status_summary(self, device_registry):
        result = device_registry.status_summary()
        assert "total" in result
        assert "summary" in result


class TestProtocolCfg:
    def test_version_upgrade(self, protocol_cfg):
        # mock a row as 已生效
        rows = store.rows("protocol_cfg")
        for row in rows:
            if row.get("配置编号") == "PROT-0003":
                row["status"] = "已生效"
                break
        result = protocol_cfg.version_upgrade("PROT-0003", "v2.0")
        assert result["ok"] is True

    def test_validate_protocol(self, protocol_cfg):
        result = protocol_cfg.validate_protocol("PROT-0003")
        assert isinstance(result, dict)

    def test_device_protocol_distribution(self, protocol_cfg):
        result = protocol_cfg.device_protocol_distribution()
        assert isinstance(result, list)


class TestMsgDecode:
    def test_parse_message(self, msg_decode):
        result = msg_decode.parse_message("AABBCCDD", "报文解码样例3")
        assert "ok" in result

    def test_detect_protocol(self, msg_decode):
        result = msg_decode.detect_protocol("AABBCCDD")
        assert result["ok"] is True
        assert "matched_count" in result


class TestMsgRouter:
    def test_match_topics(self, msg_router):
        result = msg_router.match_topics("消息路由样例3")
        assert isinstance(result, list)

    def test_route_message(self, msg_router):
        result = msg_router.route_message("消息路由样例3", {"key": "val"})
        assert isinstance(result, dict)

    def test_routing_statistics(self, msg_router):
        result = msg_router.routing_statistics()
        assert "total_rules" in result


class TestTsPoint:
    def test_simulate_data(self, ts_point):
        result = ts_point.simulate_data("TS_P-0002", count=5)
        if result["ok"]:
            assert len(result["data"]) == 5

    def test_batch_collect_status(self, ts_point):
        result = ts_point.batch_collect_status()
        assert "total_points" in result


class TestTsStore:
    def test_estimate_storage_no_strategy(self, ts_store):
        result = ts_store.estimate_storage("NONEXIST")
        assert result["ok"] is False

    def test_optimize_retention(self, ts_store):
        result = ts_store.optimize_retention()
        assert "total_estimated_mb" in result


class TestDownsample:
    def test_execute_downsample_nonexistent(self, downsample):
        result = downsample.execute_downsample("NONEXIST")
        assert result["ok"] is False

    def test_quality_check(self, downsample):
        result = downsample.quality_check("DOWN-0001")
        assert result["ok"] is True
        assert result["round_id"] == "R1"
        assert result["mean"] == 20.0


# ========================
# 新增跨模块方法测试
# ========================

class TestAlarmTriggerCrossModule:
    def test_evaluate_with_rule_engine_no_trigger(self, alarm_trigger):
        result = alarm_trigger.evaluate_with_rule_engine("NONEXIST", 50)
        assert "triggered" in result

    def test_correlate_with_device_status(self, alarm_trigger):
        result = alarm_trigger.correlate_with_device_status("DEVI-0001")
        assert result["ok"] is True
        assert "设备状态" in result


class TestRuleEngineCrossModule:
    def test_execute_chain_for_alarm_nonexistent(self, rule_engine):
        result = rule_engine.execute_chain_for_alarm("NONEXIST", "NONEXIST")
        assert result["ok"] is False

    def test_conflict_check_with_alarms(self, rule_engine):
        result = rule_engine.conflict_check_with_alarms("RULE-0003")
        assert result["ok"] is True


class TestShadowDevCrossModule:
    def test_sync_shadow_with_tags(self, shadow_dev):
        result = shadow_dev.sync_shadow_with_tags("SHAD-0001", "active")
        assert result["ok"] is True

    def test_tag_aware_shadow_summary(self, shadow_dev):
        result = shadow_dev.tag_aware_shadow_summary()
        assert isinstance(result, list)


class TestTagBindingCrossModule:
    def test_bind_tag_with_shadow_sync(self, tag_binding):
        result = tag_binding.bind_tag_with_shadow_sync("SHAD-0001", "status", "active")
        assert "ok" in result

    def test_query_device_with_shadow(self, tag_binding):
        result = tag_binding.query_device_with_shadow("SHAD-0001")
        assert result["ok"] is True


class TestGatewayMgmtCrossModule:
    def test_manage_cache_for_gateway(self, gateway_mgmt):
        result = gateway_mgmt.manage_cache_for_gateway("GATE-0001", "256MB", "LRU")
        assert result["ok"] is True

    def test_device_distribution_by_gateway(self, gateway_mgmt):
        result = gateway_mgmt.device_distribution_by_gateway()
        assert isinstance(result, list)


class TestEdgeCacheCrossModule:
    def test_adjust_cache_by_gateway(self, edge_cache):
        result = edge_cache.adjust_cache_by_gateway("EDGE-0003")
        assert result["ok"] is True

    def test_invalidate_by_gateway_health(self, edge_cache):
        result = edge_cache.invalidate_by_gateway_health("GATE-0001")
        assert result["ok"] is True


class TestFirmwareOtaCrossModule:
    def test_ota_with_aggregate_reporting(self, firmware_ota):
        result = firmware_ota.ota_with_aggregate_reporting("DEVI-0001", "v2.0.1", "rolling")
        assert result["ok"] is True
        assert "ota_task" in result
        assert "aggregate_task" in result

    def test_check_device_registry_for_ota(self, firmware_ota):
        result = firmware_ota.check_device_registry_for_ota("DEVI-0001")
        assert result["ok"] is True


class TestAggregateSinkCrossModule:
    def test_aggregate_ota_progress(self, aggregate_sink):
        # Setup: ensure there's an OTA task
        result = aggregate_sink.aggregate_ota_progress("FIRM-0001", 5)
        assert result["ok"] is True

    def test_dispatch_to_batch_export(self, aggregate_sink):
        result = aggregate_sink.dispatch_to_batch_export("AGGR-0001", "JSON")
        assert result["ok"] is True
        assert "export_task" in result


class TestDataStreamCrossModule:
    def test_stream_with_export_not_running(self, data_stream):
        result = data_stream.stream_with_export("DATA-0001", "设备1", "测点1")
        assert result["ok"] is False

    def test_check_downstream_export_status(self, data_stream):
        result = data_stream.check_downstream_export_status("DATA-0003")
        assert result["ok"] is True
        assert "stream_status" in result


class TestBatchExportCrossModule:
    def test_export_from_stream_source_not_running(self, batch_export):
        result = batch_export.export_from_stream_source("DATA-0001", "设备1", "测点1")
        assert result["ok"] is False

    def test_estimate_from_ts_store(self, batch_export):
        result = batch_export.estimate_from_ts_store("TS_S-0001", 30)
        assert "ok" in result


# ========================
# CrossModuleService 综合测试
# ========================

class TestCrossModuleService:
    def test_register_device_with_protocol(self):
        # Ensure PROT-0003 is in 已生效 state (may have been modified by version_upgrade test)
        protocol_rows = store.rows("protocol_cfg")
        for row in protocol_rows:
            if row.get("配置编号") == "PROT-0003":
                row["status"] = "已生效"
                break
        result = cross_service.register_device_with_protocol("NEW-DEV-01", "温湿度传感器", "PROT-0003")
        assert result["ok"] is True

    def test_register_device_duplicate(self):
        result = cross_service.register_device_with_protocol("DEVI-0001", "温湿度传感器", "PROT-0003")
        assert result["ok"] is False

    def test_query_device_protocol_status(self):
        result = cross_service.query_device_protocol_status()
        assert isinstance(result, list)

    def test_decode_and_route(self):
        result = cross_service.decode_and_route("temp=25,humidity=60,pressure=1013", "报文解码样例3", "消息路由样例3")
        assert result["ok"] is True

    def test_configure_point_with_store(self):
        result = cross_service.configure_point_with_store("NEW-PT-01", "DEVI-0001", "温度测点", "tsdb")
        assert result["ok"] is True

    def test_query_point_store_matrix(self):
        result = cross_service.query_point_store_matrix()
        assert isinstance(result, list)

    def test_downsample_with_gap_check(self):
        result = cross_service.downsample_with_gap_check("TS_P-0001", "5m", "avg")
        assert result["ok"] is True

    def test_evaluate_alarm_with_rule(self):
        result = cross_service.evaluate_alarm_with_rule("NONEXIST", 100)
        assert "triggered" in result

    def test_sync_device_shadow_with_tags(self):
        result = cross_service.sync_device_shadow_with_tags("SHAD-0001", "active")
        assert result["ok"] is True

    def test_bind_tag_with_shadow_check(self):
        result = cross_service.bind_tag_with_shadow_check("SHAD-0001", "env", "prod")
        assert result["ok"] is True

    def test_manage_gateway_cache(self):
        result = cross_service.manage_gateway_cache("GATE-0001", "512MB", "LRU")
        assert result["ok"] is True

    def test_query_gateway_cache_matrix(self):
        result = cross_service.query_gateway_cache_matrix()
        assert isinstance(result, list)

    def test_ota_with_aggregate_dispatch(self):
        result = cross_service.ota_with_aggregate_dispatch("DEVI-0001", "v3.0.0", "canary")
        assert result["ok"] is True
        assert "aggregate_task" in result

    def test_query_ota_aggregate_status(self):
        result = cross_service.query_ota_aggregate_status()
        assert isinstance(result, list)

    def test_stream_to_batch_export_not_running(self):
        result = cross_service.stream_to_batch_export("DATA-0001", "区域A", "测点A,测点B")
        assert result["ok"] is False

    def test_query_stream_export_matrix(self):
        result = cross_service.query_stream_export_matrix()
        assert isinstance(result, list)

    def test_cross_module_overview(self):
        result = cross_service.cross_module_overview()
        assert "total_modules" in result
        assert "flows" in result