"""Architecture boundary tests for module dependency direction."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_domain_layer_does_not_depend_on_other_project_layers() -> None:
    """Keep domain contracts/models free from infrastructure dependencies."""

    violations = _collect_import_violations(
        package_dir=REPO_ROOT / "domain",
        forbidden_prefixes=("api", "ingestion", "sources"),
    )
    assert violations == []


def test_ingestion_layer_does_not_depend_on_api_layer() -> None:
    """Keep ingestion logic independent of CLI/presentation layer."""

    violations = _collect_import_violations(
        package_dir=REPO_ROOT / "ingestion",
        forbidden_prefixes=("api",),
    )
    assert violations == []


def _collect_import_violations(package_dir: Path, forbidden_prefixes: tuple[str, ...]) -> list[str]:
    violations: list[str] = []
    for path in sorted(package_dir.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_forbidden(alias.name, forbidden_prefixes):
                        violations.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno} imports {alias.name}")
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                if _is_forbidden(node.module, forbidden_prefixes):
                    violations.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno} imports {node.module}")
    return violations


def _is_forbidden(module_name: str, forbidden_prefixes: tuple[str, ...]) -> bool:
    return any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in forbidden_prefixes)
