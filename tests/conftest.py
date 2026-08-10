"""Lets tests import dags.returns_pipeline without apache-airflow installed.

apache-airflow is only present inside the Airflow container image (it's not
in requirements-airflow.txt), so a bare `import airflow` fails in a plain
local/CI Python environment. returns_pipeline.py only uses `DAG` and
`PythonOperator` to wire up the module-level DAG object; none of the
functions under test call into them. Stub just enough of the airflow API so
the module imports cleanly, but only when the real package isn't available
so real Airflow runs (e.g. inside the container) are unaffected.
"""

from __future__ import annotations

import sys
import types

try:
    import airflow  # noqa: F401
except ImportError:

    class _FakeOperator:
        def __init__(self, *args, **kwargs):
            self.task_id = kwargs.get("task_id")

        def __rshift__(self, other):
            return other

        def __rrshift__(self, other):
            return self

    class _FakeDAG:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

    airflow_module = types.ModuleType("airflow")
    airflow_module.DAG = _FakeDAG
    sys.modules["airflow"] = airflow_module

    operators_module = types.ModuleType("airflow.operators")
    sys.modules["airflow.operators"] = operators_module

    operators_python_module = types.ModuleType("airflow.operators.python")
    operators_python_module.PythonOperator = _FakeOperator
    sys.modules["airflow.operators.python"] = operators_python_module
