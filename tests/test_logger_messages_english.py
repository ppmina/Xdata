from __future__ import annotations

import ast
from pathlib import Path

LOG_METHODS = {"debug", "info", "warning", "error", "exception", "critical", "log"}
SCOPE_NODES = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef)
BLOCKED_SCOPE_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


def _is_logger_call(node: ast.Call) -> bool:
    return isinstance(node.func, ast.Attribute) and node.func.attr in LOG_METHODS and bool(node.args)


def _extract_literal_text(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value

    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
        return "".join(parts)

    return None


def _contains_non_ascii(text: str) -> bool:
    return any(ord(char) > 127 for char in text)


def _iter_nodes_in_scope(scope: ast.AST):
    for child in ast.iter_child_nodes(scope):
        if isinstance(child, BLOCKED_SCOPE_NODES):
            continue
        yield child
        yield from _iter_nodes_in_scope(child)


def _collect_local_message_assignments(scope: ast.AST) -> dict[str, tuple[str, int]]:
    assignments: dict[str, tuple[str, int]] = {}
    for node in _iter_nodes_in_scope(scope):
        value_node: ast.AST | None = None
        target_name: str | None = None

        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target_name = node.targets[0].id
            value_node = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name = node.target.id
            value_node = node.value

        if target_name is None or value_node is None:
            continue

        literal_text = _extract_literal_text(value_node)
        if literal_text is not None:
            assignments[target_name] = (literal_text, node.lineno)

    return assignments


def _iter_scopes(module: ast.Module):
    yield module
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            yield node


def test_logger_messages_are_english_only() -> None:
    src_root = Path(__file__).resolve().parents[1] / "src" / "cryptoservice"
    failures: list[str] = []

    for file_path in sorted(src_root.rglob("*.py")):
        source = file_path.read_text(encoding="utf-8")
        module = ast.parse(source)

        for scope in _iter_scopes(module):
            local_assignments = _collect_local_message_assignments(scope)

            for node in _iter_nodes_in_scope(scope):
                if not isinstance(node, ast.Call) or not _is_logger_call(node):
                    continue

                message_node = node.args[0]
                message_text: str | None = None
                line_no = node.lineno

                if isinstance(message_node, ast.Name):
                    resolved = local_assignments.get(message_node.id)
                    if resolved is not None:
                        message_text, line_no = resolved
                else:
                    message_text = _extract_literal_text(message_node)

                if message_text is None or not _contains_non_ascii(message_text):
                    continue

                snippet = " ".join(message_text.strip().split())
                if len(snippet) > 140:
                    snippet = f"{snippet[:137]}..."
                relative = file_path.relative_to(Path(__file__).resolve().parents[1])
                failures.append(f"{relative}:{line_no}: {snippet!r}")

    assert not failures, "Non-English logger messages detected:\n" + "\n".join(failures)
