from __future__ import annotations

import ast
from pathlib import Path


def test_concrete_producers_are_not_imported_from_producer_client() -> None:
    root = Path(__file__).resolve().parents[1]
    src_root = root / "src"
    target_module = "src.infra.messaging.connect.producer_client"

    violations: list[str] = []

    for path in src_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != target_module:
                continue

            imported_names = [alias.name for alias in node.names]
            invalid_names = [name for name in imported_names if name != "AvroProducer"]
            if invalid_names:
                violations.append(
                    f"{path.relative_to(root)}:{node.lineno} imports {invalid_names} "
                    "from producer_client"
                )

    assert not violations, "\n".join(violations)
