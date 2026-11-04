"""Actual-entry research mode, import boundary and zero-I/O proofs."""

import ast
from dataclasses import fields
import hashlib
import importlib.util
import builtins
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, cast
import pytest
from src.core.backtest import spider_historical_execution as execution
from src.core.backtest import spider_historical_input as historical_input_module
from src.core.backtest import spider_historical_replay as historical_replay
from src.core.backtest import spider_historical_research_entry as entry
from src.core.backtest import spider_historical_research_output as research_output
from src.core.backtest import spider_run_admission as admission
from src.core.backtest import spider_run_evidence as evidence
from src.core.backtest import spider_run_store as store
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_historical_input import HistoricalInputError
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_scenario_run import frozen_h03_input, install_frozen_h03_order, read

_BASE = Path(__file__).parents[1] / "src/core/backtest"
_ENTRY = "spider_historical_research_entry"
_ALLOWED = {
    "spider_historical_execution",
    "spider_historical_ports",
    "spider_historical_input",
    "spider_historical_replay",
    "spider_historical_report_projection",
    "spider_historical_research_entry",
    "spider_historical_research_output",
    "spider_policy_protocol",
    "spider_policy",
    "spider_run_artifacts",
    "spider_run_errors",
    "synthetic_scenario_codec",
    "synthetic_scenario_replay",
    "synthetic_policy_dispatch",
    "spider_paid_endpoint",
    "spider_execution_input",
    "spider_gross_response",
    "spider_gross_execution_input",
    "spider_floating_exit_host",
    "spider_floating_exit_input",
    "spider_floating_exit_policy",
    "spider_bayes_model",
    "spider_bayes_replay",
}
_FORBIDDEN_MODULES = {
    "spider_scenario_run",
    "spider_run_store",
    "spider_run_admission",
    "spider_run_evidence",
    "spider_historical_durable_ports",
}
_ALLOWED_EXTERNAL_MODULES = {
    "__future__",
    "collections.abc",
    "copy",
    "dataclasses",
    "datetime",
    "decimal",
    "enum",
    "fluxtrade_core",
    "functools",
    "hashlib",
    "heapq",
    "importlib.machinery",
    "json",
    "re",
    "typing",
}
_ALLOWED_EXTERNAL_IMPORTS = {
    "__future__": {"annotations"},
    "collections.abc": {"Mapping", "Sequence"},
    "copy": {"deepcopy"},
    "dataclasses": {"asdict", "dataclass", "field"},
    "datetime": {"datetime"},
    "decimal": {
        "Clamped",
        "Context",
        "Decimal",
        "DivisionByZero",
        "FloatOperation",
        "InvalidOperation",
        "Overflow",
        "ROUND_CEILING",
        "ROUND_FLOOR",
        "ROUND_HALF_EVEN",
        "localcontext",
    },
    "enum": {"Enum"},
    "functools": {"wraps"},
    "hashlib": {"sha256"},
    "importlib.machinery": {"EXTENSION_SUFFIXES"},
    "json": set(),
    "heapq": set(),
    "re": set(),
    "typing": {
        "Any",
        "Callable",
        "Literal",
        "NamedTuple",
        "NotRequired",
        "Protocol",
        "Sequence",
        "TypeAlias",
        "TypedDict",
        "TypeVar",
        "cast",
    },
    "src.core.decimal_math": {"canonical_decimal_text"},
    "src.core.backtest.spider_historical_input": {
        "BAR_DURATION_MS",
        "HistoricalBar",
        "HistoricalInputError",
        "HistoricalRunInput",
        "POLL_OPEN_ORDERS_FAILURE_PROFILE",
        "_decode_historical_run_input_validated",
        "_historical_context_for_validated_input",
        "_UnapprovedHistoricalInputTree",
        "_canonical",
        "_validate_configured_spec_timeline",
        "decode_historical_run_input",
        "decode_p2_configuration",
        "encode_historical_run_input",
        "historical_context_for_input",
        "historical_market_step_clock",
        "historical_market_step_in_range",
        "validate_historical_input",
    },
    "src.core.backtest.spider_run_artifacts": {
        "_HASHES",
        "_decimal_text",
        "canonical_bytes",
        "configuration_context",
        "decode_canonical",
        "historical_context",
    },
}
_FORBIDDEN_CAPABILITY_NAMES = {
    "builtins",
    "__builtins__",
    "getattr",
    "setattr",
    "delattr",
    "__import__",
    "open",
    "eval",
    "exec",
    "compile",
    "globals",
    "locals",
    "vars",
    "breakpoint",
    "input",
    "memoryview",
    "os",
    "io",
    "shutil",
    "tempfile",
    "pathlib",
    "socket",
    "subprocess",
    "sqlite3",
    "importlib",
}
_BUILTIN_ALLOWLIST = {
    "any",
    "bool",
    "bytes",
    "dict",
    "enumerate",
    "Exception",
    "int",
    "isinstance",
    "len",
    "list",
    "max",
    "min",
    "next",
    "range",
    "RuntimeError",
    "sorted",
    "staticmethod",
    "str",
    "tuple",
    "type",
    "zip",
}
_ALLOWED_IMPORTED_MODULE_ATTRIBUTES = {
    ("src.core.backtest.synthetic_scenario_codec", "_native_failure"),
}
_NEW_MODULES = (
    "spider_historical_ports",
    "spider_historical_research_output",
    "spider_historical_research_entry",
)
_RESEARCH_OWNED_MODULES = (
    "spider_historical_research_entry",
    "spider_historical_research_output",
    "spider_historical_ports",
)
_ALLOWED_CORE = {f"src.core.backtest.{name}" for name in _ALLOWED} | {"src.core.decimal_math"}
_ENTRY_IMPORTS = {
    "typing.Any",
    "typing.cast",
    "src.core.backtest.synthetic_scenario_codec",
    "src.core.backtest.spider_historical_execution._execute_historical",
    "src.core.backtest.spider_historical_input.HistoricalInputError",
    "src.core.backtest.spider_historical_input._decode_historical_run_input_validated",
    "src.core.backtest.spider_historical_input._historical_context_for_validated_input",
    "src.core.backtest.spider_historical_input._UnapprovedHistoricalInputTree",
    "src.core.backtest.spider_historical_input.decode_historical_run_input",
    "src.core.backtest.spider_historical_input.decode_p2_configuration",
    "src.core.backtest.spider_historical_input.historical_context_for_input",
    "src.core.backtest.spider_historical_input.validate_historical_input",
    "src.core.backtest.spider_historical_research_output.ResearchEvidenceSink",
    "src.core.backtest.spider_historical_research_output.ResearchFailureResult",
    "src.core.backtest.spider_historical_research_output.ResearchRunResult",
    "src.core.backtest.spider_run_artifacts.configuration_context",
    "src.core.backtest.spider_run_errors.classify_historical_failure",
}
_OUTPUT_IMPORTS = {
    "dataclasses.dataclass",
    "typing.Any",
    "typing.cast",
    "src.core.backtest.spider_historical_ports.EvidenceSink",
    "src.core.backtest.spider_historical_report_projection.FinalOwnerEvidenceValues",
    "src.core.backtest.spider_historical_report_projection.project_historical_report",
}
_PORTS_IMPORTS = {"typing.Any", "typing.Protocol"}
_RESEARCH_IMPORTS = {
    "spider_historical_research_entry": _ENTRY_IMPORTS,
    "spider_historical_research_output": _OUTPUT_IMPORTS,
    "spider_historical_ports": _PORTS_IMPORTS,
}
_RESEARCH_PURE_MODULES = {"__future__", "dataclasses", "typing"}
_RESEARCH_INTERNAL_MODULES = {
    "src.core.backtest.spider_historical_execution",
    "src.core.backtest.spider_historical_input",
    "src.core.backtest.spider_historical_ports",
    "src.core.backtest.spider_historical_report_projection",
    "src.core.backtest.spider_historical_research_output",
    "src.core.backtest.spider_run_artifacts",
    "src.core.backtest.spider_run_errors",
    "src.core.backtest.synthetic_scenario_codec",
}
_HEAD_INPUT_EXCEPTION_CASES = (
    # These expected dispositions are frozen from a0996166 input-matrix-base.json.
    ("OSError", OSError, ("MATRIX_ERROR",), True),
    ("ValueError", ValueError, ("MATRIX_ERROR",), True),
    ("ValueErrorEmpty", ValueError, (), True),
    ("ValueErrorOdd", ValueError, ("ODD", "ARGS", 3), True),
    ("TypeError", TypeError, ("MATRIX_ERROR",), True),
    ("KeyError", KeyError, ("MATRIX_ERROR",), True),
    ("HistoricalInputError", HistoricalInputError, ("MATRIX_ERROR",), True),
    ("RuntimeError", RuntimeError, ("MATRIX_ERROR",), False),
    ("MemoryError", MemoryError, ("MATRIX_ERROR",), False),
    ("IndexError", IndexError, ("MATRIX_ERROR",), False),
    ("KeyboardInterrupt", KeyboardInterrupt, ("MATRIX_ERROR",), False),
    ("SystemExit", SystemExit, ("MATRIX_ERROR",), False),
    (
        "ScenarioReplayInputError",
        wire._native.ScenarioReplayInputError,
        ("MATRIX_ERROR",),
        True,
    ),
    ("ScenarioReplayInputErrorEmpty", wire._native.ScenarioReplayInputError, (), True),
    (
        "ScenarioReplayInputErrorOdd",
        wire._native.ScenarioReplayInputError,
        ("ODD", "ARGS", 3),
        True,
    ),
    (
        "ScenarioReplayLookupError",
        wire._native.ScenarioReplayLookupError,
        ("MATRIX_ERROR",),
        False,
    ),
    (
        "ScenarioReplayLookupErrorEmpty",
        wire._native.ScenarioReplayLookupError,
        (),
        False,
    ),
    (
        "ScenarioReplayLookupErrorOdd",
        wire._native.ScenarioReplayLookupError,
        ("ODD", "ARGS", 3),
        False,
    ),
    (
        "ScenarioReplayConflictError",
        wire._native.ScenarioReplayConflictError,
        ("MATRIX_ERROR",),
        True,
    ),
    (
        "ScenarioReplayConflictErrorEmpty",
        wire._native.ScenarioReplayConflictError,
        (),
        True,
    ),
    (
        "ScenarioReplayConflictErrorOdd",
        wire._native.ScenarioReplayConflictError,
        ("ODD", "ARGS", 3),
        True,
    ),
    (
        "ScenarioReplayInvariantError",
        wire._native.ScenarioReplayInvariantError,
        ("MATRIX_ERROR",),
        False,
    ),
    (
        "ScenarioReplayInvariantErrorEmpty",
        wire._native.ScenarioReplayInvariantError,
        (),
        False,
    ),
    (
        "ScenarioReplayInvariantErrorOdd",
        wire._native.ScenarioReplayInvariantError,
        ("ODD", "ARGS", 3),
        False,
    ),
    (
        "ReconciliationProjectionError",
        evidence.ReconciliationProjectionError,
        ("MATRIX_ERROR",),
        True,
    ),
    ("SpiderRunStoreError", store.SpiderRunStoreError, ("MATRIX_ERROR",), False),
)
_HEAD_INPUT_PHASES = (
    ("_decode_historical_input_validated", "_decode_historical_run_input_validated"),
    ("__module_validate_historical_input", "__module_validate_historical_input"),
    ("_decode_p2_configuration", "decode_p2_configuration"),
    ("_historical_context_for_validated_input", "_historical_context_for_validated_input"),
)


class _StoreErrorChild(store.SpiderRunStoreError):
    pass


def _local_modules(source_override: dict[str, str] | None = None) -> dict[str, str]:
    sources = source_override or {}
    pending, found = ([_ENTRY], {})
    while pending:
        name = pending.pop()
        if name in found:
            continue
        source = sources.get(name, (_BASE / f"{name}.py").read_text())
        found[name] = source
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    module = importlib.util.resolve_name("." * node.level + module, "src.core.backtest")
                targets = [module]
                if module == "src.core.backtest":
                    targets.extend((f"{module}.{alias.name}" for alias in node.names))
            else:
                continue
            for target in targets:
                if target == "src.core.backtest":
                    continue
                if target.startswith("src.core."):
                    name = target.rsplit(".", 1)[-1]
                    if name in _FORBIDDEN_MODULES:
                        raise AssertionError(f"forbidden research import: {name}")
                    if target not in _ALLOWED_CORE:
                        raise AssertionError(f"unlisted research import: {target}")
                    if target.startswith("src.core.backtest."):
                        pending.append(name)
    assert set(found) <= _ALLOWED
    return found


def _runtime_import_name(node: ast.ImportFrom) -> str:
    module = node.module or ""
    if node.level:
        return importlib.util.resolve_name("." * node.level + module, "src.core.backtest")
    return module


def _assert_runtime_static_allowlist(graph: dict[str, str]) -> None:
    """Fail closed on capabilities in research-owned modules only.

    The complete transitive local-module closure is independently constrained by
    ``_local_modules``. Shared durable modules keep their established source of
    truth and are covered by the runtime no-I/O spies and filesystem snapshots.
    """
    assert set(_RESEARCH_OWNED_MODULES) <= set(graph)
    for module_name in _RESEARCH_OWNED_MODULES:
        source = graph[module_name]
        _assert_research_module_names(source, module_name)
        assert _imports(source) <= _RESEARCH_IMPORTS[module_name], (
            module_name,
            sorted(_imports(source) - _RESEARCH_IMPORTS[module_name]),
        )
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Name) and node.id in _FORBIDDEN_CAPABILITY_NAMES:
                if isinstance(node.ctx, ast.Load):
                    raise AssertionError(f"forbidden capability name: {node.id} in {module_name}")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "open":
                raise AssertionError(f"forbidden writer callable in {module_name}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported = alias.name
                    if imported not in _RESEARCH_PURE_MODULES | _RESEARCH_INTERNAL_MODULES:
                        raise AssertionError(f"unlisted runtime module: {imported}")
            elif isinstance(node, ast.ImportFrom):
                imported = _runtime_import_name(node)
                if imported not in _RESEARCH_PURE_MODULES | _RESEARCH_INTERNAL_MODULES | {"src.core.backtest"}:
                    raise AssertionError(f"unlisted runtime module: {imported}")


class _ResearchScope:
    def __init__(self, parent, kind: str, bindings: set[str], globals_: set[str], nonlocals: set[str]):
        self.parent = parent
        self.kind = kind
        self.bindings = bindings
        self.globals = globals_
        self.nonlocals = nonlocals

    def resolves(self, name: str) -> bool:
        if name in self.globals:
            return self.module_scope().has_binding(name)
        if name in self.nonlocals:
            parent = self.parent
            while parent is not None and parent.kind == "class":
                parent = parent.parent
            return parent is not None and parent.kind != "module" and parent.resolves(name)
        if name in self.bindings:
            return True
        parent = self.parent
        if self.kind == "function" and parent is not None and parent.kind == "class":
            parent = parent.parent
        if parent is not None:
            return parent.resolves(name)
        return name in _BUILTIN_ALLOWLIST

    def has_binding(self, name: str) -> bool:
        if self.kind == "module":
            return name in self.bindings
        return self.parent is not None and self.parent.has_binding(name)

    def module_scope(self):
        scope = self
        while scope.parent is not None:
            scope = scope.parent
        return scope


class _ScopeBindings(ast.NodeVisitor):
    """Collect bindings for one Python scope without descending into child scopes."""

    def __init__(self):
        self.bindings: set[str] = set()
        self.globals: set[str] = set()
        self.nonlocals: set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, (ast.Store, ast.Param)):
            self.bindings.add(node.id)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.bindings.add(alias.asname or alias.name.split(".")[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name != "*":
                self.bindings.add(alias.asname or alias.name)

    def visit_Global(self, node: ast.Global) -> None:
        self.globals.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        self.nonlocals.update(node.names)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name is not None:
            self.bindings.add(node.name)
        if node.type is not None:
            self.visit(node.type)
        for statement in node.body:
            self.visit(statement)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.bindings.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.bindings.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.bindings.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return None

    def visit_ListComp(self, node: ast.ListComp) -> None:
        return None

    def visit_SetComp(self, node: ast.SetComp) -> None:
        return None

    def visit_DictComp(self, node: ast.DictComp) -> None:
        return None

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        return None


class _ResearchNameResolver(ast.NodeVisitor):
    def __init__(self, module_name: str, tree: ast.Module):
        self.module_name = module_name
        self.module_scope = self._make_scope(None, "module", tree.body, None)
        self.scope = self.module_scope
        self.imported_modules = self._module_aliases(tree)
        self.parents = {id(child): parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        self.annotation_depth = 0
        self.visited_name_loads: set[int] = set()
        self.visited_attributes: set[int] = set()

    @staticmethod
    def _module_aliases(tree: ast.Module) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    aliases[alias.asname or alias.name.split(".")[0]] = alias.name
            elif isinstance(node, ast.ImportFrom):
                module = _runtime_import_name(node)
                for alias in node.names:
                    if module == "src.core.backtest" and alias.name in _ALLOWED:
                        aliases[alias.asname or alias.name] = f"{module}.{alias.name}"
        changed = True
        while changed:
            changed = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    value = node.value
                    targets = node.targets
                elif isinstance(node, ast.AnnAssign):
                    value = node.value
                    targets = [node.target]
                else:
                    continue
                if not isinstance(value, ast.Name) or value.id not in aliases:
                    continue
                for target in targets:
                    for name in ast.walk(target):
                        if isinstance(name, ast.Name) and name.id not in aliases:
                            aliases[name.id] = aliases[value.id]
                            changed = True
        return aliases

    @staticmethod
    def _make_scope(parent, kind: str, body, arguments) -> _ResearchScope:
        collector = _ScopeBindings()
        for statement in body:
            collector.visit(statement)
        if arguments is not None:
            for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs):
                collector.bindings.add(argument.arg)
            if arguments.vararg is not None:
                collector.bindings.add(arguments.vararg.arg)
            if arguments.kwarg is not None:
                collector.bindings.add(arguments.kwarg.arg)
        collector.bindings.difference_update(collector.globals | collector.nonlocals)
        return _ResearchScope(parent, kind, collector.bindings, collector.globals, collector.nonlocals)

    def _check_attribute(self, node: ast.Attribute) -> None:
        self.visited_attributes.add(id(node))
        if node.attr.startswith("__") and node.attr.endswith("__"):
            raise AssertionError(f"dunder attribute access is forbidden: {node.attr} in {self.module_name}")
        value = node.value
        while isinstance(value, ast.Attribute):
            value = value.value
        if isinstance(value, ast.Name) and value.id in self.imported_modules:
            pair = (self.imported_modules[value.id], node.attr)
            if pair not in _ALLOWED_IMPORTED_MODULE_ATTRIBUTES:
                raise AssertionError(f"unlisted imported-module attribute: {pair} in {self.module_name}")

    def visit_Name(self, node: ast.Name) -> None:
        if not isinstance(node.ctx, ast.Load):
            return
        self.visited_name_loads.add(id(node))
        if node.id in _FORBIDDEN_CAPABILITY_NAMES:
            raise AssertionError(f"forbidden capability name: {node.id} in {self.module_name}")
        if not self.scope.resolves(node.id):
            raise AssertionError(f"unresolved name: {node.id} in {self.module_name}:{node.lineno}")
        if node.id in self.imported_modules and not self._module_reference_is_allowed(node):
            raise AssertionError(
                f"imported module used as a value: {node.id} in {self.module_name}:{node.lineno}"
            )

    def _module_reference_is_allowed(self, node: ast.Name) -> bool:
        if self.annotation_depth:
            return True
        parent = self.parents.get(id(node))
        if not isinstance(parent, ast.Attribute) or parent.value is not node:
            return False
        pair = (self.imported_modules[node.id], parent.attr)
        return pair in _ALLOWED_IMPORTED_MODULE_ATTRIBUTES

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self._check_attribute(node)
        self.generic_visit(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for expression in (*node.decorator_list, *node.args.defaults, *node.args.kw_defaults):
            if expression is not None:
                self.visit(expression)
        for argument in (
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
            node.args.vararg,
            node.args.kwarg,
        ):
            if argument is not None and argument.annotation is not None:
                self._visit_annotation(argument.annotation)
        if node.returns is not None:
            self._visit_annotation(node.returns)
        child = self._make_scope(self.scope, "function", node.body, node.args)
        parent, self.scope = self.scope, child
        for statement in node.body:
            self.visit(statement)
        self.scope = parent

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        for expression in (*node.args.defaults, *node.args.kw_defaults):
            if expression is not None:
                self.visit(expression)
        for argument in (
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
            node.args.vararg,
            node.args.kwarg,
        ):
            if argument is not None and argument.annotation is not None:
                self._visit_annotation(argument.annotation)
        child = self._make_scope(self.scope, "function", [ast.Expr(node.body)], node.args)
        parent, self.scope = self.scope, child
        self.visit(node.body)
        self.scope = parent

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for expression in (*node.decorator_list, *node.bases, *(keyword.value for keyword in node.keywords)):
            self.visit(expression)
        child = self._make_scope(self.scope, "class", node.body, None)
        parent, self.scope = self.scope, child
        for statement in node.body:
            self.visit(statement)
        self.scope = parent

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.target)
        self._visit_annotation(node.annotation)
        if node.value is not None:
            self.visit(node.value)

    def _visit_annotation(self, node: ast.expr) -> None:
        annotation = node
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            try:
                annotation = ast.parse(node.value, mode="eval").body
            except SyntaxError as error:
                raise AssertionError(f"unparseable string annotation in {self.module_name}") from error
        self.annotation_depth += 1
        try:
            self.visit(annotation)
        finally:
            self.annotation_depth -= 1

    def _visit_comprehension(self, node, values) -> None:
        generators = node.generators
        self.visit(generators[0].iter)
        collector = _ScopeBindings()
        for generator in generators:
            collector.visit(generator.target)
        child = _ResearchScope(self.scope, "comprehension", collector.bindings, set(), set())
        parent, self.scope = self.scope, child
        self.visit(generators[0].target)
        for condition in generators[0].ifs:
            self.visit(condition)
        for generator in generators[1:]:
            self.visit(generator.iter)
            self.visit(generator.target)
            for condition in generator.ifs:
                self.visit(condition)
        for value in values:
            self.visit(value)
        self.scope = parent

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node, (node.elt,))

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node, (node.elt,))

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node, (node.elt,))

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node, (node.key, node.value))


def _assert_research_module_names(source: str, module_name: str) -> None:
    tree = ast.parse(source)
    resolver = _ResearchNameResolver(module_name, tree)
    resolver.visit(tree)
    expected_names = {
        id(node) for node in ast.walk(tree) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    }
    expected_attributes = {id(node) for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert expected_names <= resolver.visited_name_loads, (module_name, "unvisited Name Load")
    assert expected_attributes <= resolver.visited_attributes, (module_name, "unvisited Attribute")


def _imports(source: str) -> set[str]:
    result = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            result.update((alias.name for alias in node.names))
        elif isinstance(node, ast.ImportFrom):
            module = (
                importlib.util.resolve_name("." * node.level + (node.module or ""), "src.core.backtest")
                if node.level
                else node.module or ""
            )
            result.update((f"{module}.{alias.name}" for alias in node.names))
    return result


def _p3_digest() -> str:
    digest = hashlib.sha256()
    for name in sorted(scenario._P3_PROGRAM):
        raw_name = name.encode()
        digest.update(len(raw_name).to_bytes(8, "big") + raw_name)
        digest.update(hashlib.sha256((scenario._ROOT / name).read_bytes()).digest())
    return digest.hexdigest()


def _small_input(name: str = "c6-research") -> tuple[Any, bytes]:
    return frozen_h03_input(name)


def _assert_boundary(result):
    boundary = result.last_processed_boundary
    if boundary is None:
        assert result.last_fully_recorded_row_ordinal is None
        return
    assert set(boundary) == {"ordinal", "kind", "event_key"}
    assert type(boundary["ordinal"]) is int and boundary["ordinal"] > 0
    assert boundary["kind"] and boundary["event_key"]
    assert (
        result.last_fully_recorded_row_ordinal is None
        or result.last_fully_recorded_row_ordinal <= boundary["ordinal"]
    )


def _raise(error):
    def fail(*_args, **_kwargs):
        raise error

    return fail


def _classified_error(case):
    primary = {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"}
    native = {
        "native_input": wire._native.ScenarioReplayInputError,
        "native_lookup": wire._native.ScenarioReplayLookupError,
        "native_conflict": wire._native.ScenarioReplayConflictError,
        "native_invariant": wire._native.ScenarioReplayInvariantError,
    }
    if case in native:
        return native[case]("CLASSIFIED_FAILURE")
    if case == "projection":
        return evidence.ReconciliationProjectionError()
    error_type = _StoreErrorChild if case == "store_subclass" else store.SpiderRunStoreError
    reason = "PUBLICATION_DURABILITY_UNKNOWN" if case == "store_durability" else "PUBLICATION_FAILED"
    return error_type(reason, None if case == "store" else "REQUESTED_FAILURE", primary)


def test_research_entry_executes_real_fixture_and_matches_durable_order(tmp_path, monkeypatch):
    value, raw = _small_input()
    install_frozen_h03_order(monkeypatch)
    mode = ["durable"]
    snapshots: dict[str, list[str]] = {"durable": [], "research": []}
    durable_rows, research_rows = ([], [])
    original_requests = execution._owner_requests
    original_dretain = scenario._DurableHistoricalOutput.retain
    original_rretain = research_output.ResearchEvidenceSink.retain
    durable_finish_inputs, research_finish_inputs = ([], [])
    validation_calls = [0]
    validated_preparations = {"durable": 0, "research": 0}
    original_validate = historical_input_module.validate_historical_input
    original_configuration = scenario._historical_configuration
    original_prepare = historical_replay._prepare_historical_replay_validated

    def counted_validate(run):
        validation_calls[0] += 1
        return original_validate(run)

    def counted_configuration(*args, **kwargs):
        before = validation_calls[0]
        result = original_configuration(*args, **kwargs)
        assert validation_calls[0] - before == 1
        return result

    def counted_prepare(token):
        validated_preparations[mode[0]] += 1
        before = validation_calls[0]
        result = original_prepare(token)
        assert validation_calls[0] == before
        return result

    monkeypatch.setattr(historical_input_module, "validate_historical_input", counted_validate)
    monkeypatch.setattr(scenario, "_validate_historical_input", counted_validate)
    monkeypatch.setattr(historical_replay, "validate_historical_input", counted_validate)
    monkeypatch.setattr(historical_replay, "_prepare_historical_replay_validated", counted_prepare)
    monkeypatch.setattr(scenario, "_historical_configuration", counted_configuration)
    original_dfinish = scenario._DurableHistoricalOutput.finish
    original_rfinish = research_output.ResearchEvidenceSink.finish

    def requests(account, cutoff, prefix):
        rows = original_requests(account, cutoff, prefix)
        snapshots[mode[0]].extend((row["snapshot_id"] for row in rows))
        return rows

    def durable_retain(self, row):
        durable_rows.append(row)
        return original_dretain(self, row)

    def research_retain(self, row):
        research_rows.append(row)
        return original_rretain(self, row)

    def durable_finish(self, *args):
        durable_finish_inputs.append(args)
        return original_dfinish(self, *args)

    def research_finish(self, *args):
        research_finish_inputs.append(args)
        return original_rfinish(self, *args)

    monkeypatch.setattr(execution, "_owner_requests", requests)
    monkeypatch.setattr(scenario._DurableHistoricalOutput, "retain", durable_retain)
    monkeypatch.setattr(research_output.ResearchEvidenceSink, "retain", research_retain)
    monkeypatch.setattr(scenario._DurableHistoricalOutput, "finish", durable_finish)
    monkeypatch.setattr(research_output.ResearchEvidenceSink, "finish", research_finish)
    assert scenario.run_spider_scenario(str(tmp_path), value.run_id, scenario._P3_SELECTOR, raw) == {
        "run_id": value.run_id,
        "outcome": "ADMITTED",
        "reason": None,
    }
    assert validated_preparations["durable"] == 1
    durable_endpoint = read(tmp_path, value.run_id, "endpoint.json")[0]
    durable_report = read(tmp_path, value.run_id, "report.jsonl")
    durable_journal = read(tmp_path, value.run_id, "journal.jsonl")
    mode[0] = "research"
    research_before = validation_calls[0]
    result = entry.run_historical_research(raw)
    assert isinstance(result, research_output.ResearchRunResult)
    assert validation_calls[0] - research_before == 1
    assert (result.research_classification, result.admission_state) == (
        "RESEARCH_ONLY",
        "NOT_ADMITTED",
    )
    assert result.report_rows == tuple(durable_report)
    assert result.final_owner_evidence == durable_endpoint["final_owner_evidence"]
    assert result.final_scheduler_observation == durable_endpoint["scheduler_observation"]
    assert durable_rows == research_rows == durable_journal
    assert snapshots["durable"] == snapshots["research"]
    assert len(durable_finish_inputs) == len(research_finish_inputs) == 1
    assert len(durable_finish_inputs[0]) == len(research_finish_inputs[0]) == 5
    assert durable_finish_inputs[0] == research_finish_inputs[0]
    assert any(
        (
            fill["source_event_id"]
            for row in research_rows
            if row["record_kind"] == "HISTORICAL_MARKET_RESULT"
            for product in row["payload"]["result"]["products"]
            for fill in product["fills"]
        )
    )


def test_research_entry_falls_back_to_raw_validation_for_unapproved_token_tree(monkeypatch):
    _value, raw = _small_input("research-token-fallback")
    install_frozen_h03_order(monkeypatch)
    calls = [0]
    original = historical_input_module.validate_historical_input

    def counted(run):
        calls[0] += 1
        return original(run)

    def non_exact_tree(_raw):
        raise historical_input_module._UnapprovedHistoricalInputTree(
            "historical input tree is not an approved immutable DTO graph"
        )

    monkeypatch.setattr(entry, "_decode_historical_run_input_validated", non_exact_tree)
    monkeypatch.setattr(historical_input_module, "validate_historical_input", counted)
    monkeypatch.setattr(entry, "validate_historical_input", counted)
    monkeypatch.setattr(historical_replay, "validate_historical_input", counted)

    result = entry.run_historical_research(raw)

    assert isinstance(result, research_output.ResearchRunResult)
    assert calls[0] == 5


def test_failure_dto_schema_has_no_economic_fields():
    names = {item.name for item in fields(research_output.ResearchFailureResult)}
    required = {
        "failure_classification",
        "reason",
        "primary",
        "last_processed_boundary",
        "last_fully_recorded_row_ordinal",
        "research_classification",
        "admission_state",
    }
    assert required <= names
    assert not names & {
        "report_rows",
        "fills",
        "references",
        "pnl",
        "equity",
        "positions",
        "final_owner_evidence",
        "economic_projection",
    }


@pytest.mark.parametrize(
    "case",
    [
        "malformed",
        "policy",
        OSError,
        ValueError,
        TypeError,
        KeyError,
        HistoricalInputError,
    ],
)
def test_head_input_rejections_match_durable_outcomes(tmp_path, monkeypatch, case):
    # Frozen a0996166: the P3 configuration catch tuple returns REJECTED/UNSUPPORTED_CONFIGURATION.
    value, raw = _small_input("reject-" + (case if isinstance(case, str) else case.__name__))
    if case == "malformed":
        raw = b"{}"
    elif case == "policy":
        monkeypatch.setattr(scenario, "_P3_POLICY_SOURCE", "0" * 64)
        monkeypatch.setattr(entry, "_POLICY_SOURCE", "0" * 64)
    else:
        fault = cast(type[Exception], case)("input rejection")
        monkeypatch.setattr(scenario, "_decode_historical_input_validated", _raise(fault))
        monkeypatch.setattr(entry, "_decode_historical_run_input_validated", _raise(fault))
    durable = scenario.run_spider_scenario(str(tmp_path), value.run_id, scenario._P3_SELECTOR, raw)
    result = entry.run_historical_research(raw)
    assert isinstance(result, research_output.ResearchFailureResult)
    assert (durable["outcome"], durable["reason"]) == (result.outcome, result.reason)
    assert (
        result.outcome,
        result.failure_classification,
        result.reason,
        result.primary,
    ) == ("REJECTED", "UNSUPPORTED_CONFIGURATION", "UNSUPPORTED_CONFIGURATION", None)
    assert result.last_processed_boundary is result.last_fully_recorded_row_ordinal is None
    assert not tuple(tmp_path.rglob("*"))


def _input_fault(error_type, args):
    return error_type(*args)


def _observe_input_call(call, fault):
    try:
        result = call()
    except BaseException as error:
        return ("raise", type(error), error.args, error is fault)
    if isinstance(result, dict):
        return ("return", result["outcome"], result["reason"])
    return ("return", result.outcome, result.reason)


@pytest.mark.parametrize(
    "durable_hook,research_hook",
    _HEAD_INPUT_PHASES,
    ids=("decode", "validate", "configuration", "context"),
)
@pytest.mark.parametrize(
    "case,error_type,args,head_returns",
    _HEAD_INPUT_EXCEPTION_CASES,
    ids=[case[0] for case in _HEAD_INPUT_EXCEPTION_CASES],
)
def test_input_exception_phase_matrix_matches_a0996166(
    tmp_path,
    durable_hook,
    research_hook,
    case,
    error_type,
    args,
    head_returns,
):
    """Freeze the durable return-versus-raise contract at every input phase."""
    value, raw = _small_input("phase-matrix-" + case)
    durable_fault = _input_fault(error_type, args)
    research_fault = _input_fault(error_type, args)
    with pytest.MonkeyPatch.context() as scope:
        if durable_hook == "__module_validate_historical_input":
            scope.setattr(historical_input_module, "validate_historical_input", _raise(durable_fault))
        else:
            scope.setattr(scenario, durable_hook, _raise(durable_fault))
        durable = _observe_input_call(
            lambda: scenario.run_spider_scenario(
                str(tmp_path),
                value.run_id,
                scenario._P3_SELECTOR,
                raw,
            ),
            durable_fault,
        )
    with pytest.MonkeyPatch.context() as scope:
        if research_hook == "__module_validate_historical_input":
            scope.setattr(historical_input_module, "validate_historical_input", _raise(research_fault))
        else:
            scope.setattr(entry, research_hook, _raise(research_fault))
        research = _observe_input_call(
            lambda: entry.run_historical_research(raw),
            research_fault,
        )
    if head_returns:
        expected = ("return", "REJECTED", "UNSUPPORTED_CONFIGURATION")
    else:
        expected = ("raise", error_type, tuple(args), True)
    assert durable == expected, (case, durable_hook)
    assert research == expected, (case, research_hook)


@pytest.mark.parametrize(
    "case",
    [
        "native_input",
        "native_lookup",
        "native_conflict",
        "native_invariant",
        "projection",
        "store",
        "store_requested",
        "store_durability",
        "store_subclass",
    ],
)
def test_classified_exception_classes_match_head_return_vs_research_dto(tmp_path, monkeypatch, case):
    value, raw = _small_input("classified-" + case)
    primary = {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"}
    native = {"native_input", "native_lookup", "native_conflict", "native_invariant"}
    expected: dict[str, Any] = {"outcome": "FAILED", "classification": "NATIVE_FAULT"}
    if case in native:
        fault = _classified_error(case)
        expected["primary"] = {"kind": "NATIVE", "reason": "CLASSIFIED_FAILURE"}
    elif case == "projection":
        fault = _classified_error(case)
        expected.update(classification="ENDPOINT_RECONCILIATION_FAILED", primary=None)
    else:
        reason = "PUBLICATION_DURABILITY_UNKNOWN" if case == "store_durability" else "PUBLICATION_FAILED"
        fault = _classified_error(case)
        expected.update(
            outcome="DURABILITY_UNKNOWN" if case == "store_durability" else "FAILED",
            classification="DURABILITY_UNKNOWN" if case == "store_durability" else "PERSISTENCE_FAILED",
            reason=reason,
            primary=primary,
        )
    monkeypatch.setattr(_ReplayComposition, "_capture_owner_evidence_core", _raise(fault))
    install_frozen_h03_order(monkeypatch)
    durable = scenario.run_spider_scenario(str(tmp_path), value.run_id, scenario._P3_SELECTOR, raw)
    result = entry.run_historical_research(raw)
    assert isinstance(result, research_output.ResearchFailureResult)
    assert (durable["outcome"], durable["reason"]) == (result.outcome, result.reason)
    assert result.outcome == expected["outcome"]
    assert result.failure_classification == expected["classification"]
    assert result.reason == expected.get(
        "reason", "NATIVE_FAULT" if case in native else "ENDPOINT_RECONCILIATION_FAILED"
    )
    assert result.primary == expected["primary"]
    assert result.last_processed_boundary is None and result.last_fully_recorded_row_ordinal is None


def _exercise_failure_selector(monkeypatch, case):
    _value, raw = _small_input("c6-" + case)
    install_frozen_h03_order(monkeypatch)
    fault = wire._native.ScenarioReplayInvariantError(case.upper() + "_FAULT")
    filled, captures, calls = ([], [], [])
    original_core = _ReplayComposition._capture_owner_evidence_core
    original_capture = _ReplayComposition.capture_owner_evidence
    original_market = wire.ScenarioCodec.historical_market_step
    if case == "initial":

        def fail_initial(self, cutoff, *requests):
            if requests[0]["snapshot_id"].startswith("INITIAL-"):
                raise fault
            return original_core(self, cutoff, *requests)

        monkeypatch.setattr(_ReplayComposition, "_capture_owner_evidence_core", fail_initial)
    elif case in ("checkpoint", "persist"):
        monkeypatch.setattr(
            research_output.ResearchEvidenceSink,
            case,
            lambda *_args, **_kwargs: (_ for _ in ()).throw(fault),
        )
    elif case == "filled_capture":

        def observe_market(self, node):
            result = original_market(self, node)
            filled.extend(
                (fill["source_event_id"] for product in result["products"] for fill in product["fills"])
            )
            return result

        def fail_market_capture(self, cutoff, *requests):
            captures.extend((request["snapshot_id"] for request in requests))
            if requests[0]["snapshot_id"].startswith("MARKET-") and filled:
                raise fault
            return original_core(self, cutoff, *requests)

        monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", observe_market)
        monkeypatch.setattr(_ReplayComposition, "_capture_owner_evidence_core", fail_market_capture)
    elif case == "engine":

        def fail_engine(self, node):
            calls.append(node)
            if len(calls) == 3:
                raise fault
            return original_market(self, node)

        monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", fail_engine)
    elif case == "final_capture":

        def fail_final(self, cutoff, *requests):
            captures.extend((request["snapshot_id"] for request in requests))
            if requests[0]["snapshot_id"].startswith("FINAL-"):
                raise fault
            return original_capture(self, cutoff, *requests)

        monkeypatch.setattr(_ReplayComposition, "capture_owner_evidence", fail_final)
    elif case == "finish":
        original_requests = execution._owner_requests

        def trace_requests(account, cutoff, prefix):
            rows = original_requests(account, cutoff, prefix)
            captures.extend((row["snapshot_id"] for row in rows))
            return rows

        monkeypatch.setattr(execution, "_owner_requests", trace_requests)
        projection_error = evidence.ReconciliationProjectionError()

        def fail_projection(**_kwargs):
            raise projection_error

        monkeypatch.setattr(research_output, "project_historical_report", fail_projection)
    elif case == "generic":
        fault = RuntimeError("ordinary P3 failure")
        monkeypatch.setattr(_ReplayComposition, "_dispatch_due", lambda *_: (_ for _ in ()).throw(fault))
    else:
        fault = MemoryError("retain must escape")
        monkeypatch.setattr(
            research_output.ResearchEvidenceSink,
            "retain",
            lambda *_: (_ for _ in ()).throw(fault),
        )
    if case in ("generic", "retain_memory"):
        expected_args = tuple(fault.args)
        with pytest.raises(type(fault)) as caught:
            entry.run_historical_research(raw)
        assert caught.value is fault and caught.value.args == expected_args
        return
    result = entry.run_historical_research(raw)
    assert isinstance(result, research_output.ResearchFailureResult)
    assert result.admission_state == "NOT_ADMITTED"
    if case == "initial":
        assert (result.failure_classification, result.reason, result.primary) == (
            "NATIVE_FAULT",
            "NATIVE_FAULT",
            {"kind": "NATIVE", "reason": "INITIAL_FAULT"},
        )
        assert result.last_processed_boundary is result.last_fully_recorded_row_ordinal is None
        assert result.research_classification == "RESEARCH_ONLY"
    elif case in ("checkpoint", "persist"):
        assert result.reason == "PERSISTENCE_FAILED"
        assert result.last_processed_boundary == dict(
            ordinal=1,
            kind="HISTORICAL_MARKET_RESULT",
            event_key="P3_MARKET_1790640000000_0",
        )
        assert result.last_fully_recorded_row_ordinal is None
    elif case == "filled_capture":
        assert filled and result.reason == "PERSISTENCE_FAILED"
        assert result.last_fully_recorded_row_ordinal is not None
        assert not any((name.startswith("FINAL-") for name in captures))
        assert not {"report_rows", "fills", "equity"} & {item.name for item in fields(result)}
    elif case == "engine":
        assert len(calls) == 3 and result.reason == "NATIVE_FAULT"
    elif case == "final_capture":
        assert result.reason == "NATIVE_FAULT"
        assert captures[-3:] == [
            "FINAL-TRADING",
            "FINAL-POSITIONS",
            "FINAL-OPEN-ORDERS",
        ]
    else:
        assert result.failure_classification == "ENDPOINT_RECONCILIATION_FAILED"
        assert captures[-3:] == [
            "FINAL-TRADING",
            "FINAL-POSITIONS",
            "FINAL-OPEN-ORDERS",
        ]
    _assert_boundary(result)


def _frontier_error(kind):
    if kind == "native":
        return wire._native.ScenarioReplayInvariantError("FRONTIER_NATIVE")
    if kind == "persistence":
        return store.SpiderRunStoreError(
            "ARTIFACT_WRITE_FAILED",
            None,
            {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"},
        )
    if kind == "durability_unknown":
        return store.SpiderRunStoreError(
            "PUBLICATION_DURABILITY_UNKNOWN",
            None,
            {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"},
        )
    if kind == "projection":
        return evidence.ReconciliationProjectionError()
    return RuntimeError("FRONTIER_ORDINARY")


def _raise_on_frontier(monkeypatch, frontier, fault):
    if frontier.startswith("checkpoint"):
        original = research_output.ResearchEvidenceSink.checkpoint
        target_call = 1 if frontier == "checkpoint_first" else 3
        calls = []

        def fail_checkpoint(self, boundary, kind):
            calls.append(boundary["ordinal"])
            if len(calls) == target_call:
                raise fault
            return original(self, boundary, kind)

        monkeypatch.setattr(
            research_output.ResearchEvidenceSink,
            "checkpoint",
            fail_checkpoint,
        )
    elif frontier == "persist":
        monkeypatch.setattr(research_output.ResearchEvidenceSink, "persist", _raise(fault))
    elif frontier == "retain":
        monkeypatch.setattr(research_output.ResearchEvidenceSink, "retain", _raise(fault))
    else:
        original = _ReplayComposition.capture_owner_evidence

        def fail_final(self, cutoff, *requests):
            if requests[0]["snapshot_id"].startswith("FINAL-"):
                raise fault
            return original(self, cutoff, *requests)

        monkeypatch.setattr(_ReplayComposition, "capture_owner_evidence", fail_final)


@pytest.mark.parametrize(
    "frontier",
    ["checkpoint_first", "checkpoint_later", "persist", "retain", "final_capture"],
)
@pytest.mark.parametrize(
    "failure_kind",
    ["native", "persistence", "durability_unknown", "projection", "ordinary"],
)
def test_every_failure_frontier_kind_preserves_allocated_boundary(
    monkeypatch,
    frontier,
    failure_kind,
):
    value, raw = _small_input(f"frontier-{frontier}-{failure_kind}")
    install_frozen_h03_order(monkeypatch)
    fault = _frontier_error(failure_kind)
    allocated = []
    original_note = research_output.ResearchEvidenceSink.note_boundary

    def trace_boundary(self, boundary, kind):
        original_note(self, boundary, kind)
        allocated.append(
            dict(
                ordinal=boundary["ordinal"],
                kind=kind,
                event_key=boundary["barrier_id"],
            )
        )

    monkeypatch.setattr(
        research_output.ResearchEvidenceSink,
        "note_boundary",
        trace_boundary,
    )
    _raise_on_frontier(monkeypatch, frontier, fault)
    try:
        result = entry.run_historical_research(raw)
    except BaseException as error:
        result = error

    callback_frontier = frontier.startswith(("checkpoint", "persist"))
    expected_return = failure_kind != "ordinary" or callback_frontier
    if not expected_return:
        assert isinstance(result, BaseException)
        assert result is fault
        assert result.args == fault.args
        return

    assert isinstance(result, research_output.ResearchFailureResult)
    assert allocated
    boundary = result.last_processed_boundary
    assert boundary == allocated[-1]
    assert boundary is not None
    assert set(boundary) == {"ordinal", "kind", "event_key"}
    if frontier == "final_capture":
        assert result.last_fully_recorded_row_ordinal == allocated[-1]["ordinal"]
    elif frontier == "checkpoint_later":
        assert allocated[-1]["ordinal"] == 3
        assert result.last_fully_recorded_row_ordinal == 2
    else:
        assert allocated[-1]["ordinal"] == 1
        assert result.last_fully_recorded_row_ordinal is None

    if failure_kind == "durability_unknown":
        assert (result.outcome, result.failure_classification, result.reason) == (
            "DURABILITY_UNKNOWN",
            "DURABILITY_UNKNOWN",
            "PUBLICATION_DURABILITY_UNKNOWN",
        )
    elif failure_kind == "projection" and not callback_frontier:
        assert (result.outcome, result.failure_classification, result.reason) == (
            "FAILED",
            "ENDPOINT_RECONCILIATION_FAILED",
            "ENDPOINT_RECONCILIATION_FAILED",
        )
    elif failure_kind == "native" and not callback_frontier:
        assert (result.outcome, result.failure_classification, result.reason) == (
            "FAILED",
            "NATIVE_FAULT",
            "NATIVE_FAULT",
        )
    elif failure_kind == "persistence" and not callback_frontier:
        assert (result.outcome, result.failure_classification, result.reason) == (
            "FAILED",
            "PERSISTENCE_FAILED",
            "ARTIFACT_WRITE_FAILED",
        )
    else:
        assert (result.outcome, result.failure_classification, result.reason) == (
            "FAILED",
            "PERSISTENCE_FAILED",
            "PERSISTENCE_FAILED",
        )


@pytest.mark.parametrize("case", ["generic", "retain_memory"])
def test_rethrown_exception_args_oracle_kills_mutation(monkeypatch, case):
    original = entry.run_historical_research

    def mutate_args(raw):
        try:
            return original(raw)
        except (RuntimeError, MemoryError) as error:
            error.args = ("MUTATED_ARGUMENTS",)
            raise

    monkeypatch.setattr(entry, "run_historical_research", mutate_args)
    with pytest.raises(AssertionError):
        _exercise_failure_selector(monkeypatch, case)


def _filesystem_listing(root):
    return tuple(sorted(((str(path.relative_to(root)), path.is_dir()) for path in root.rglob("*"))))


def _run_noio_case(tmp_path, monkeypatch, case):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(home))
    before, writers = (_filesystem_listing(tmp_path), [])

    def deny(*args, **kwargs):
        writers.append((args, kwargs))
        raise AssertionError("research path reached a writer")

    def guard_open(original):
        def guarded(file, mode="r", *args, **kwargs):
            if any((flag in mode for flag in "wax+")):
                return deny(file, mode, *args, **kwargs)
            return original(file, mode, *args, **kwargs)

        return guarded

    path_open, builtin_open, io_open = (Path.open, builtins.open, io.open)
    for name in (
        "create",
        "register",
        "mark_processed",
        "append_journal",
        "publish_failure",
        "finalize",
    ):
        monkeypatch.setattr(store.SpiderRunStore, name, deny)
    monkeypatch.setattr(store, "_open", deny)
    for name in (
        "open",
        "link",
        "replace",
        "rename",
        "mkdir",
        "makedirs",
        "unlink",
        "remove",
    ):
        monkeypatch.setattr(os, name, deny)
    monkeypatch.setattr(builtins, "open", guard_open(builtin_open))
    monkeypatch.setattr(io, "open", guard_open(io_open))
    monkeypatch.setattr(Path, "open", guard_open(path_open))
    for name in ("write_text", "write_bytes", "mkdir", "touch", "unlink", "replace"):
        monkeypatch.setattr(Path, name, deny)
    for name in ("mkstemp", "mkdtemp", "NamedTemporaryFile", "TemporaryDirectory"):
        monkeypatch.setattr(tempfile, name, deny)
    monkeypatch.setattr(admission, "admit_spider_run", deny)
    monkeypatch.setattr(evidence, "build_endpoint_artifacts", deny)
    raised = None
    try:
        if case == "success":
            install_frozen_h03_order(monkeypatch)
            entry.run_historical_research(_small_input("noio-success")[1])
        elif case == "invalid_input":
            assert entry.run_historical_research(b"{}").outcome == "REJECTED"
            value, raw = _small_input("noio-policy-rejected")
            monkeypatch.setattr(entry, "_POLICY_SOURCE", "0" * 64)
            assert entry.run_historical_research(raw).outcome == "REJECTED"
        elif case == "classified":
            for failure in (
                "native_input",
                "native_lookup",
                "native_conflict",
                "native_invariant",
                "projection",
                "store",
                "store_requested",
                "store_durability",
                "store_subclass",
            ):
                error = _classified_error(failure)
                with pytest.MonkeyPatch.context() as scope:
                    scope.setattr(
                        _ReplayComposition,
                        "_capture_owner_evidence_core",
                        _raise(error),
                    )
                    install_frozen_h03_order(scope)
                    result = entry.run_historical_research(_small_input("noio-" + failure)[1])
                    assert isinstance(result, research_output.ResearchFailureResult)
                assert _filesystem_listing(tmp_path) == before
        else:
            _exercise_failure_selector(monkeypatch, case)
    except BaseException as error:
        raised = error
    assert _filesystem_listing(tmp_path) == before
    assert not writers, "research path reached a writer"
    if raised is not None:
        raise raised


@pytest.mark.parametrize(
    "case",
    [
        "success",
        "invalid_input",
        "classified",
        "initial",
        "checkpoint",
        "persist",
        "filled_capture",
        "engine",
        "final_capture",
        "finish",
        "generic",
        "retain_memory",
    ],
)
def test_research_entry_has_no_durable_io_on_every_exit(tmp_path, monkeypatch, case):
    _run_noio_case(tmp_path, monkeypatch, case)


def test_runtime_pathlib_writer_mutation_is_rejected(tmp_path, monkeypatch):
    original = research_output.ResearchEvidenceSink.finish

    def leak(self, *args):
        Path("unexpected.txt").write_text("forbidden")
        return original(self, *args)

    monkeypatch.setattr(research_output.ResearchEvidenceSink, "finish", leak)
    with pytest.raises(AssertionError, match="research path reached a writer"):
        _run_noio_case(tmp_path, monkeypatch, "success")
    assert not (tmp_path / "unexpected.txt").exists()


def test_research_import_closure_and_forbidden_import_mutation():
    graph = _local_modules()
    assert _FORBIDDEN_MODULES.isdisjoint(graph)
    _assert_runtime_static_allowlist(graph)
    assert _imports(graph["spider_historical_research_entry"]) <= _ENTRY_IMPORTS
    assert _imports(graph["spider_historical_research_output"]) <= _OUTPUT_IMPORTS
    needle = "            token = _decode_historical_run_input_validated(raw)"
    for imported in (
        "from src.core.backtest.spider_run_store import SpiderRunStore",
        "from .spider_run_store import SpiderRunStore",
        "from . import spider_run_store as store_module",
    ):
        mutated = graph[_ENTRY].replace(needle, f"            {imported}\n{needle}", 1)
        with pytest.raises(AssertionError, match="forbidden research import"):
            _local_modules({_ENTRY: mutated})
    allowed = sorted(_ALLOWED_CORE | {"src.core", "src.core.backtest"})
    run_input = "/Volumes/Dev/Scratch/agents/spider_throughput/S0/fin-a/fin/historical_input.json"
    script = f"import sys; from pathlib import Path; from src.core.backtest import spider_historical_research_entry as entry; result=entry.run_historical_research(Path({run_input!r}).read_bytes().rstrip(b'\\n')); assert result.outcome == 'COMPLETE'; allowed={allowed!r}; loaded={{name for name in sys.modules if name.startswith('src.core.')}}; assert loaded <= set(allowed), sorted(loaded - set(allowed))"
    process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_BASE.parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode == 0, process.stderr


def test_static_writer_allowlist_rejects_all_review_mutations():
    graph = _local_modules()
    _assert_runtime_static_allowlist(graph)
    mutations = {
        "getattr os open": "import os\ngetattr(os, 'open')('x', 1)",
        "shutil copy": "import shutil\nshutil.copy('a', 'b')",
        "temporary file": "import tempfile\ntempfile.TemporaryFile()",
        "late module import alias": (
            "def write_late():\n    reviewer_open('x', 'w')\n"
            "from builtins import open as reviewer_open\nwrite_late()"
        ),
        "builtin callable alias": "writer = open\nwriter('unexpected.txt', 'w')",
        "eval writer": "eval(\"open('unexpected.txt', 'w')\")",
        "builtins mapping lookup": "__builtins__['open']('unexpected.txt', 'w')",
        "dunder traversal": "def inspect_type(value):\n    return type(value).__dict__",
        "imported module alias attribute": "codec = _wire\ncodec.open('unexpected.txt', 'w')",
        "unknown parameter annotation": "def leak(value: reviewer_unknown):\n    pass",
        "string parameter annotation": "def leak(value: 'reviewer_unknown'):\n    pass",
        "dunder parameter annotation": "def leak(value: type(1).__dict__):\n    pass",
        "annotation writer": (
            "def leak(value: _wire.json.codecs.open('unexpected.txt', 'w').close()):\n    pass"
        ),
        "module through dict": ("{'codec': _wire}['codec'].json.codecs.open('unexpected.txt', 'w').close()"),
        "module through default": (
            "def leak(codec=_wire):\n    codec.json.codecs.open('unexpected.txt', 'w').close()\nleak()"
        ),
        "module through lambda": (
            "(lambda codec: codec.json.codecs.open('unexpected.txt', 'w').close())(_wire)"
        ),
        "module through walrus": ("(codec := _wire).json.codecs.open('unexpected.txt', 'w').close()"),
        "dynamic importlib": ("import importlib\nimportlib.import_module('os').open('x', 1)"),
        "path writer": "from pathlib import Path\nPath('x').write_text('bad')",
        "unlisted socket": "import socket\nsocket.socket()",
        "unlisted callable": "from tempfile import TemporaryFile\nTemporaryFile()",
    }
    for name, source in mutations.items():
        mutated = dict(graph)
        mutated[_ENTRY] += "\n" + source + "\n"
        with pytest.raises(AssertionError):
            _assert_runtime_static_allowlist(mutated)


def test_research_runtime_modules_have_exact_p3_manifest_entries_and_digests(
    monkeypatch,
):
    expected = 29
    assert len(scenario._P3_PROGRAM) == len(set(scenario._P3_PROGRAM)) == expected
    paths = {Path(name).stem: name for name in scenario._P3_PROGRAM}
    assert all((name in paths for name in _NEW_MODULES))
    original = Path.read_bytes
    base = _p3_digest()
    for module in _NEW_MODULES:
        target = scenario._ROOT / paths[module]

        def changed(path: Path, *, _target=target) -> bytes:
            data = original(path)
            return data + b"\n# digest-only mutation\n" if path == _target else data

        monkeypatch.setattr(Path, "read_bytes", changed)
        assert _p3_digest() != base, module
        monkeypatch.setattr(Path, "read_bytes", original)
