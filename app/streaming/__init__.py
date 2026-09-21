"""Streaming layer: data ingestion sources, fraud processing pipeline, and sinks."""

from app.streaming.processor import FraudPipeline, PipelineResult
from app.streaming.sink import print_decisions, print_stats
from app.streaming.source import json_line, read_kafka, read_socket

__all__ = [
    "FraudPipeline",
    "PipelineResult",
    "json_line",
    "print_decisions",
    "print_stats",
    "read_kafka",
    "read_socket",
]