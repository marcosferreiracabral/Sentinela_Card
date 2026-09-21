"""Sentinela_Card real-time credit card fraud detection engine."""

import os
import sys
from pathlib import Path

from app.config import Config, load_config

__version__ = "1.0.0"
__all__ = ["Config", "load_config", "__version__"]


def _setup_windows_java() -> None:
    """Configures Java and Spark runtime environment variables for Windows platform."""
    if sys.platform == "win32":
        candidates = [
            os.environ.get("JAVA_HOME", ""),
            r"C:\Program Files\Eclipse Adoptium\jdk-21.0.10.7-hotspot",
            r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot",
            r"C:\Program Files\Java\jdk-25.0.2",
            r"C:\Program Files\Java\latest",
        ]
        valid_java_home: str | None = None
        for cand in candidates:
            if cand and os.path.exists(cand) and os.path.exists(os.path.join(cand, "bin", "java.exe")):
                valid_java_home = cand
                break

        if not valid_java_home:
            import shutil

            java_exe = shutil.which("java")
            if java_exe and os.path.exists(java_exe):
                resolved_parent = Path(java_exe).resolve().parent.parent
                if os.path.exists(resolved_parent / "bin" / "java.exe"):
                    valid_java_home = str(resolved_parent)

        if valid_java_home is not None:
            resolved_home = valid_java_home
            try:
                import ctypes

                buffer = ctypes.create_unicode_buffer(500)
                if ctypes.windll.kernel32.GetShortPathNameW(valid_java_home, buffer, 500) and buffer.value:
                    resolved_home = str(buffer.value)
            except (OSError, AttributeError, ImportError):
                pass
            os.environ["JAVA_HOME"] = resolved_home
            bin_dir = os.path.join(resolved_home, "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    if not os.environ.get("SPARK_HOME"):
        try:
            import pyspark.find_spark_home as fsh

            os.environ["SPARK_HOME"] = fsh._find_spark_home()
        except (ImportError, AttributeError, OSError):
            pass


_setup_windows_java()