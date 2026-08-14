"""产物校验 (文档 §3.4d D8).

默认校验 = 存在; 可加规则: check:json (可解析) / min_bytes: N (非空).
done 判定 = rc=0 且所有 artifacts 校验通过.
指纹 (A2) 与内容校验正交: 指纹管"代码版本匹配", 规则管"产物有效".
"""

from __future__ import annotations

import json
import os
from typing import Any


class ArtifactError(Exception):
    pass


def check_artifact(path: str, rule: dict[str, Any] | None) -> str | None:
    """校验单个产物. 返回 None=通过, 字符串=失败原因."""
    if not os.path.isfile(path):
        return "不存在"
    rule = rule or {}
    if rule.get("min_bytes") is not None:
        size = os.path.getsize(path)
        if size < int(rule["min_bytes"]):
            return f"过小 ({size} < {rule['min_bytes']} bytes)"
    if rule.get("check") == "json":
        try:
            with open(path, "r", encoding="utf-8") as f:
                json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            return f"JSON 解析失败: {e}"
    return None


def check_artifacts(artifacts: dict[str, dict]) -> dict[str, str | None]:
    """校验全部产物. 返回 {key: None|失败原因}."""
    result: dict[str, str | None] = {}
    for key, a in artifacts.items():
        result[key] = check_artifact(str(a["path"]), a)
    return result


def all_pass(result: dict[str, str | None]) -> bool:
    return all(v is None for v in result.values())


def validate_json_artifact(path: str) -> dict | None:
    """读取 json 产物 (如 optuna best.json), 失败返回 None."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
