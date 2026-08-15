"""Dev-time stubs for Databricks / PySpark APIs.

Call `import dev_stubs; dev_stubs.activate()` at the start of local runs/tests
to provide lightweight stand-ins for `pyspark` and the notebook `display()`.
"""
from __future__ import annotations

import builtins
import sys
import types
from typing import Any


def activate() -> None:
    """Inject minimal pyspark and display objects into the runtime.

    This does not provide full Spark functionality — only enough so imports
    and simple calls (for linting, mypy, and unit testing basic logic) work
    locally without a Databricks cluster.
    """
    # Create pyspark package modules
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")

    # Minimal column / literal helpers
    def col(name: str) -> str:  # type: ignore
        return f"COL({name})"

    def lit(value: Any) -> Any:  # type: ignore
        return value

    def udf(func, returnType=None):  # type: ignore
        return func

    functions.col = col
    functions.lit = lit
    functions.udf = udf

    # Minimal SparkSession stub
    class SparkSession:
        class Builder:
            def __init__(self) -> None:
                self._name = "dev"

            def appName(self, name: str):
                self._name = name
                return self

            def getOrCreate(self):
                return SparkSession()

        @classmethod
        def builder(cls):
            return SparkSession.Builder()

        def createDataFrame(self, data, schema=None):
            # Return the raw data for local tests (not a real DataFrame)
            return data

    sql.SparkSession = SparkSession

    # Register modules in sys.modules so `import pyspark.sql.functions as F` works
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions

    # Provide a simple `display` function commonly used in notebooks
    def _display(obj: Any) -> None:
        try:
            # If it's an iterable of rows (list/tuple), print summary
            if hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, dict)):
                print("display:")
                for i, r in enumerate(obj):
                    if i >= 20:
                        print("... (truncated)")
                        break
                    print(r)
            else:
                print(obj)
        except Exception:
            print(repr(obj))

    builtins.display = _display


if __name__ == "__main__":
    activate()
    print("dev_stubs activated")
