from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List


class _ProfileViewContractCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.ui_string_refs: List[str] = []
        self.uses_table_picker = False
        self.streamlit_tabs_calls = 0

    def visit_Attribute(self, node: ast.Attribute) -> Any:  # type: ignore[override]
        if isinstance(node.value, ast.Name) and node.value.id == "ui_strings":
            self.ui_string_refs.append(node.attr)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> Any:  # type: ignore[override]
        func = node.func
        if isinstance(func, ast.Name) and func.id == "stateless_table_picker":
            self.uses_table_picker = True
        elif (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "st"
            and func.attr == "tabs"
        ):
            self.streamlit_tabs_calls += 1
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

    collector = _ProfileViewContractCollector()
    collector.visit(render_node)

    return {
        "ui_strings": sorted({*collector.ui_string_refs}),
        "uses_table_picker": collector.uses_table_picker,
        "tabs_calls": collector.streamlit_tabs_calls,
    }


def test_profile_view_contract(snapshot) -> None:
    contract = _collect_profile_contract()
    snapshot.assert_match(contract, "profile_view_contract")
