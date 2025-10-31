from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List


def _unique(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


class _ConfigContractCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.headers: List[str] = []
        self.subheaders: List[str] = []
        self.buttons: List[str] = []
        self.form_buttons: List[str] = []
        self.multiselects: List[str] = []
        self.checkbox_labels: List[str] = []
        self.include_mentions: List[str] = []

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
                elif func.attr == "subheader":
                    self.subheaders.append(label)
                elif func.attr == "button":
                    self.buttons.append(label)
                elif func.attr == "form_submit_button":
                    self.form_buttons.append(label)
                elif func.attr == "multiselect":
                    self.multiselects.append(label)
                elif func.attr == "checkbox":
                    self.checkbox_labels.append(label)
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> Any:  # type: ignore[override]
        if isinstance(node.value, str) and "include" in node.value.lower():
            self.include_mentions.append(node.value)
        self.generic_visit(node)


def _collect_config_contract() -> Dict[str, Any]:
    module_path = Path(__file__).resolve().parents[1] / "streamlit_app.py"
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    render_node = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "render_config_editor":
            render_node = node
            break
    if render_node is None:
        raise AssertionError("render_config_editor not found in streamlit_app")

    collector = _ConfigContractCollector()
    collector.visit(render_node)

    return {
        "headers": _unique(collector.headers),
        "subheaders": _unique(collector.subheaders),
        "buttons": _unique(collector.buttons),
        "form_buttons": _unique(collector.form_buttons),
        "multiselects": _unique(collector.multiselects),
        "checkboxes": _unique(collector.checkbox_labels),
        "include_mentions": _unique(collector.include_mentions),
    }


def test_config_editor_contract(snapshot) -> None:
    contract = _collect_config_contract()
    snapshot.assert_match(contract, "config_editor_contract")
