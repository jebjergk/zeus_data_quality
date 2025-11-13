from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List


def _unique(values: List[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


class _ProfilePlaceholderCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.headers: List[str] = []
        self.captions: List[str] = []
        self.infos: List[str] = []

    def _strings_from_node(self, node: ast.AST | None) -> List[str]:
        if node is None:
            return []
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [node.value]
        if isinstance(node, ast.IfExp):
            values: List[str] = []
            values.extend(self._strings_from_node(node.body))
            values.extend(self._strings_from_node(node.orelse))
            return values
        return []

    def visit_Call(self, node: ast.Call) -> Any:  # type: ignore[override]
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id == "st":
            labels: List[str] = []
            if node.args:
                labels = self._strings_from_node(node.args[0])
            for label in labels:
                if func.attr == "header":
                    self.headers.append(label)
                elif func.attr == "caption":
                    self.captions.append(label)
                elif func.attr == "info":
                    self.infos.append(label)
        self.generic_visit(node)


def _collect_profile_placeholder() -> Dict[str, Any]:
    module_path = Path(__file__).resolve().parents[1] / "views" / "profile_view.py"
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    render_node = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "render_profile":
            render_node = node
            break
    if render_node is None:
        raise AssertionError("render_profile not found in views.profile_view")

    collector = _ProfilePlaceholderCollector()
    collector.visit(render_node)

    return {
        "headers": _unique(collector.headers),
        "captions": _unique(collector.captions),
        "infos": _unique(collector.infos),
    }


def test_profile_view_placeholder(snapshot) -> None:
    placeholder_contract = _collect_profile_placeholder()
    snapshot.assert_match(placeholder_contract, "profile_view_placeholder")
