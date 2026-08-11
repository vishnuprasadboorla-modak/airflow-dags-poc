import importlib.util
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parent.parent / "dags"


@pytest.mark.parametrize("dag_file", list(DAGS_DIR.glob("*.py")))
def test_dag_file_imports_without_error(dag_file):
    spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)   # raises on any DAG-definition error
