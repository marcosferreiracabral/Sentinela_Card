"""Configuration loading and validation for fraud detection rules and runtime settings."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class Thresholds:
    """Decision thresholds for fraud scoring."""

    approve_below: int = 30
    review_from: int = 30
    block_from: int = 70

    def decision(self, score: float) -> str:
        """Determines decision bucket based on cumulative risk score.

        Args:
            score: Risk score between 0.0 and 100.0.

        Returns:
            Decision string: 'approve', 'review', or 'block'.
        """
        if score < self.review_from:
            return "approve"
        if score < self.block_from:
            return "review"
        return "block"


@dataclass
class StreamingCfg:
    """Structured streaming runtime settings."""

    watermark_seconds: int = 600
    processing_time_seconds: int = 1
    socket_host: str = "localhost"
    socket_port: int = 9999
    checkpoint_dir: str = "data/checkpoints"


@dataclass
class RuleCfg:
    """Configuration container for an individual fraud rule."""

    name: str
    enabled: bool = True
    weight: float = 30.0
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class Config:
    """Global application configuration container."""

    model_version: str
    thresholds: Thresholds
    scoring: dict[str, Any]
    streaming: StreamingCfg
    features: dict[str, Any]
    merchant_risk: dict[str, Any]
    rules: dict[str, RuleCfg]
    data_dir: Path
    rules_path: Path

    def rule_params(self, name: str) -> dict[str, Any]:
        """Retrieves parameter dictionary for a specific rule.

        Args:
            name: Name of the fraud rule.

        Returns:
            Dictionary of parameter configurations for the rule.
        """
        return self.rules[name].params if name in self.rules else {}

    def feature(self, name: str, default: Any = None) -> Any:
        """Retrieves a feature configuration value.

        Args:
            name: Feature key name.
            default: Fallback value if key is not found.

        Returns:
            Configured feature value or default.
        """
        return self.features.get(name, default)

    def high_risk_categories(self) -> set[str]:
        """Returns set of high-risk Merchant Category Codes (MCCs).

        Returns:
            Set of MCC string codes.
        """
        return set(self.merchant_risk.get("high_risk_categories", []))

    @property
    def enabled_rules(self) -> list[RuleCfg]:
        """Returns list of all active fraud rules.

        Returns:
            List of enabled RuleCfg instances.
        """
        return [r for r in self.rules.values() if r.enabled]


def load_config(rules_path: str | Path = "rules.yaml", data_dir: str | Path = "data") -> Config:
    """Loads and validates configuration from YAML file.

    Args:
        rules_path: Path to rules.yaml definition file.
        data_dir: Base directory for data storage layers.

    Returns:
        Populated Config dataclass instance.
    """
    rules_path = Path(rules_path)
    data_dir = Path(data_dir)
    raw: dict[str, Any] = {}
    if rules_path.exists():
        with Path(rules_path).open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

    thresholds_raw = raw.get("thresholds", {})
    thresholds = Thresholds(
        approve_below=int(thresholds_raw.get("approve_below", 30)),
        review_from=int(thresholds_raw.get("review_from", 30)),
        block_from=int(thresholds_raw.get("block_from", 70)),
    )

    stream_raw = raw.get("streaming", {})
    streaming = StreamingCfg(
        watermark_seconds=int(stream_raw.get("watermark_seconds", 600)),
        processing_time_seconds=int(stream_raw.get("processing_time_seconds", 1)),
        socket_host=str(stream_raw.get("socket_host", "localhost")),
        socket_port=int(stream_raw.get("socket_port", 9999)),
        checkpoint_dir=str(stream_raw.get("checkpoint_dir", "data/checkpoints")),
    )

    rules: dict[str, RuleCfg] = {}
    for name, body in (raw.get("rules", {}) or {}).items():
        if not isinstance(body, dict):
            continue
        rules[name] = RuleCfg(
            name=name,
            enabled=bool(body.get("enabled", True)),
            weight=float(body.get("weight", 30.0)),
            params=dict(body.get("params", {}) or {}),
        )

    return Config(
        model_version=str(raw.get("model_version", "1.0.0")),
        thresholds=thresholds,
        scoring=dict(raw.get("scoring", {}) or {}),
        streaming=streaming,
        features=dict(raw.get("features", {}) or {}),
        merchant_risk=dict(raw.get("merchant_risk", {}) or {}),
        rules=rules,
        data_dir=data_dir,
        rules_path=rules_path,
    )


def save_thresholds(path: str | Path, review: int, block: int) -> None:
    """Updates review and block score thresholds in YAML file.

    Args:
        path: Path to target YAML configuration file.
        review: Minimum score threshold for review classification.
        block: Minimum score threshold for block classification.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    raw.setdefault("thresholds", {})
    raw["thresholds"]["review_from"] = review
    raw["thresholds"]["approve_below"] = review
    raw["thresholds"]["block_from"] = block
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(raw, fh, sort_keys=False, allow_unicode=True)