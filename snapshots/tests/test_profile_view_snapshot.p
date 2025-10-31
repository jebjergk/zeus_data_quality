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


class _ProfileContractCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.headers: List[str] = []
        self.captions: List[str] = []
        self.buttons: List[str] = []
        self.grid_columns: List[str] | None = None

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
                elif func.attr == "button":
                    self.buttons.append(label)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> Any:  # type: ignore[override]
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "grid_columns":
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, list) and all(isinstance(item, str) for item in value):
                    self.grid_columns = value
        self.generic_visit(node)


def _collect_profile_contract() -> Dict[str, Any]:
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

    collector = _ProfileContractCollector()
    collector.visit(render_node)

    buttons = _unique(collector.buttons)
    return {
        "headers": _unique(collector.headers),
        "captions": _unique(collector.captions),
        "buttons": buttons,
        "grid_columns": collector.grid_columns,
        "include_column_alias": "Select" if collector.grid_columns and "Select" in collector.grid_columns else None,
    }


def test_profile_view_contract(snapshot) -> None:
    contract = _collect_profile_contract()
    snapshot.assert_match(contract, "profile_view_contract")
