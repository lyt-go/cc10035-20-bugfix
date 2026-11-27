"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import cross_module, downsample, resource

# 降采样复查路由必须在通用资源路由之前注册，避免 /api/downsample 被动态路径抢先匹配。
ROUTERS = [cross_module, downsample, resource]

