"""HEAD byte and caller-oracle regressions for historical spider errors."""

import ast
import hashlib
import json
import linecache
import pickle
import sys
import traceback
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from src.core.backtest import spider_historical_execution as executor
from src.core.backtest import spider_historical_durable_ports as durable_ports
from src.core.backtest import spider_run_errors as errors
from src.core.backtest import spider_run_evidence as evidence
from src.core.backtest import spider_run_store as store
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest import synthetic_scenario_codec as wire

_HEAD_MATRIX_SHA256 = "df9d0f169032c7cfceac06422cd07d4167795f7363060edfa5c387191c4a722b"


class StoreChild(store.SpiderRunStoreError):
    pass


class ProjectionChild(evidence.ReconciliationProjectionError):
    pass


class ProjectionReasonChild(evidence.ReconciliationProjectionError):
    __module__ = "__main__"
    reason = "CHILD_REASON"


def test_moved_classes_preserve_head_bytes_and_alias_identity():
    assert store.SpiderRunStoreError is errors.SpiderRunStoreError and evidence.ReconciliationProjectionError is errors.ReconciliationProjectionError
    primary = {"kind": "NATIVE", "nested": {"reason": "fault"}}
    copied = store.SpiderRunStoreError("PUBLICATION_FAILED", "REQUESTED", primary)
    primary["nested"]["reason"] = "changed"
    assert copied.primary_failure == {"kind": "NATIVE", "nested": {"reason": "fault"}} and copied.primary_failure is not primary
    expected: tuple[tuple[Any, tuple, str, str, str, str, str], ...] = (
        (store.SpiderRunStoreError, ("PUBLICATION_FAILED",), "src.core.backtest.spider_run_store", "80049596000000000000008c227372632e636f72652e6261636b746573742e7370696465725f72756e5f73746f7265948c1353706964657252756e53746f72654572726f729493948c125055424c49434154494f4e5f4641494c454494859452947d94288c06726561736f6e9468038c187265717565737465645f6661696c7572655f726561736f6e944e8c0f7072696d6172795f6661696c757265944e75622e", "SpiderRunStoreError('PUBLICATION_FAILED')", "PUBLICATION_FAILED", "src.core.backtest.spider_run_store.SpiderRunStoreError: PUBLICATION_FAILED\n"),
        (evidence.ReconciliationProjectionError, (), "src.core.backtest.spider_run_evidence", "80049570000000000000008c257372632e636f72652e6261636b746573742e7370696465725f72756e5f65766964656e6365948c1d5265636f6e63696c696174696f6e50726f6a656374696f6e4572726f729493948c1e454e44504f494e545f5245434f4e43494c494154494f4e5f4641494c454494859452942e", "ReconciliationProjectionError('ENDPOINT_RECONCILIATION_FAILED')", "ENDPOINT_RECONCILIATION_FAILED", "src.core.backtest.spider_run_evidence.ReconciliationProjectionError: ENDPOINT_RECONCILIATION_FAILED\n"),
    )
    # Values captured from HEAD 334401c906d787a56a86cf866092c87ea4d5fb37 via review/base-probe.json.
    for cls, args, module, raw_hex, expected_repr, expected_str, only_text in expected:
        error = cast(Any, cls)(*args)
        raw = pickle.dumps(error, protocol=4)
        assert (cls.__module__, cls.__qualname__, str(type(error)), repr(error), str(error), raw.hex()) == (
            module, cls.__name__, f"<class '{module}.{cls.__name__}'>", expected_repr, expected_str, raw_hex,
        ) and type(pickle.loads(raw)) is cls
        assert "".join(traceback.format_exception_only(error)) == only_text


def test_projection_subclass_omitted_reason_matches_head():
    error = ProjectionReasonChild()
    assert error.args == ("CHILD_REASON",)
    assert str(error) == "CHILD_REASON"
    assert repr(error) == "ProjectionReasonChild('CHILD_REASON')"
    source = "try:\n raise ProjectionReasonChild()\nexcept BaseException as error:\n trace = ''.join(traceback.format_exception(error))\n"
    filename = "HEAD_projection_reason.py"
    cast(Any, linecache.cache)[filename] = (len(source), None, source.splitlines(keepends=True), filename)
    scope = {"ProjectionReasonChild": ProjectionReasonChild, "traceback": traceback}
    exec(compile(source, filename, "exec"), scope)
    # Captured by running the same child construction against HEAD 334401c9.
    assert scope["trace"] == (
        "Traceback (most recent call last):\n"
        "  File \"HEAD_projection_reason.py\", line 2, in <module>\n"
        "    raise ProjectionReasonChild()\n"
        "ProjectionReasonChild: CHILD_REASON\n"
    )


def _error_cases():
    native = wire._native.ScenarioReplayInputError
    return {"BaseExceptionGroup": BaseExceptionGroup("probe", [KeyboardInterrupt("inner")]),
            "ProjectionChild": ProjectionChild(), "ScenarioReplayInputErrorChild": type("ScenarioReplayInputErrorChild", (native,), {})("probe"),
            "StoreChild": StoreChild("PUBLICATION_DURABILITY_UNKNOWN")}


def _error_info(error, seen=None):
    seen = set() if seen is None else seen
    if id(error) in seen:
        return "<cycle>"
    seen.add(id(error))
    return dict(type=type(error).__qualname__, args=repr(error.args), reason=getattr(error, "reason", None), requested=getattr(error, "requested_failure_reason", None), primary=getattr(error, "primary_failure", None), cause=_error_info(error.__cause__, seen) if error.__cause__ else None)


def test_real_callers_match_head_error_matrix():
    matrix = {}
    for name, error in _error_cases().items():
        for state in ("RUNNING", "FROZEN", "NONE"):
            for newer in (False, True):
                events = []
                def publish_outer(reason, primary):
                    events.append(["publish", reason, primary])
                    if newer:
                        raise store.SpiderRunStoreError("PUBLICATION_FAILED", reason, primary)
                fake = type("FakeStore", (), {
                    "_state": property(lambda _: events.append(["state", state]) or state),
                    "register": lambda *_a, **_k: "NATIVE_CONSTRUCTION_ALLOWED",
                    "publish_failure": lambda _self, reason, primary: publish_outer(reason, primary),
                })()
                def create(*args):
                    if state == "NONE":
                        raise error
                    return fake
                def execute(*args):
                    raise error
                with patch.object(scenario, "_Store", SimpleNamespace(create=create)), \
                     patch.object(scenario, "_historical_configuration", return_value=(object(), {}, {})), \
                     patch.object(scenario, "_attempt", return_value={}), \
                     patch.object(scenario, "_execute_historical", execute):
                    try:
                        result = scenario.run_spider_scenario("valid", "probe", scenario._P3_SELECTOR, b"input")
                        outcome = ["return", result]
                    except BaseException as caught:
                        outcome = ["raise", _error_info(caught), caught is error]
                matrix[f"outer:{name}:{state}:newer={newer}"] = dict(outcome=outcome, events=events)
        for stage in ("mark_processed", "append_journal"):
            for newer in (False, True):
                events, closed = [], []
                composition = SimpleNamespace(_account={}, _capture_owner_evidence_core=lambda *_: {})
                composition._dispatch_due = lambda *_: (
                    composition._evidence_callback("SNAPSHOT_FACT", {"stable_id": "probe", "visible_at": 1},
                                                   {"request": {"captured_at": 1}}),
                    {"classification": "SUCCESS"},
                )[1]
                def mark(*args, **kwargs):
                    events.append(["mark"])
                    if stage == "mark_processed":
                        raise error
                def append(*args):
                    events.append(["append"])
                    raise error
                def publish_callback(reason, primary):
                    events.append(["publish", reason, primary])
                    if newer:
                        raise store.SpiderRunStoreError("PUBLICATION_FAILED", reason, primary)
                callback_output = SimpleNamespace(
                    failure_result=None, checkpoint=mark, persist=append,
                    retain=lambda _row: None,
                    note_boundary=lambda _boundary, _kind: None,
                    fail=lambda _classification, reason, primary, _boundary, _ordinal:
                        publish_callback(reason, primary),
                    finish=lambda *_args: None,
                )
                def trace(frame, event, arg):
                    if (event == "return" and frame.f_code.co_filename == executor.__file__
                            and frame.f_code.co_name in ("persist", "_execute_historical")):
                        # The callback's closed value is a closure cell. If BaseException bypasses
                        # its Exception handler, CPython may omit the untouched cell from f_locals;
                        # the enclosing executor initializes that value to False.
                        closed.append((frame.f_code.co_name, frame.f_locals.get("closed", False)))
                    return trace
                with patch.object(executor, "_ReplayComposition", SimpleNamespace(_from_historical_run=lambda run: composition)):
                    sys.settrace(trace)
                    try:
                        result = executor._execute_historical(
                            {"configuration_context": {}, "historical_context": {}},
                            cast(Any, SimpleNamespace(run_id="probe", range_start_ms=0, range_end_ms=1)),
                            cast(Any, callback_output),
                        )
                        outcome = ["return", result]
                    except BaseException as caught:
                        outcome = ["raise", _error_info(caught), caught is error]
                    finally:
                        sys.settrace(None)
                matrix[f"callback:{name}:{stage}:newer={newer}"] = dict(outcome=outcome, events=events, closed=closed)
    assert len(matrix) == 40 and hashlib.sha256(json.dumps(matrix, sort_keys=True, separators=(",", ":")).encode()).hexdigest() == _HEAD_MATRIX_SHA256  # Selected HEAD rows from S4/c3/review/scratch/base-probe.json


def test_real_store_failure_outcomes_and_artifact_bytes_match_head(tmp_path, monkeypatch):
    # HEAD 334401c9 real-store oracle: S4/c3/review2/scratch/durable-base/results.json.
    expected = "a6f646d346089156c18a18962264c5c45d1ae965b029568977180b38e7c55ab6"
    from test_spider_scenario_run import historical_input
    from tools.spider_run_compare import _EXPECTED_DIFFERENCES, _canonical
    cases = ("create", "mark_unknown", "append_oserror", "append_store", "append_memory", "append_keyboard", "append_base", "append_newer", "outer_projection", "outer_native", "outer_memory", "outer_keyboard", "outer_base", "outer_nested", "outer_newer", "retain_memory", "endpoint_unlink", "completion_unlink")
    retain_line = next(n.lineno for n in ast.walk(ast.parse(Path(durable_ports.__file__).read_text()))
                       if isinstance(n, ast.Call) and ast.unparse(n.func) == "self._journal.append")
    matrix = {}
    for case in cases:
        case_root = tmp_path / case
        case_root.mkdir()
        _, raw = historical_input("probe")
        events, observed, closed = [], [], []
        original_publish, original_mark = store.SpiderRunStore.publish_failure, store.SpiderRunStore.mark_processed
        original_append, original_unlink = store.SpiderRunStore.append_journal, Path.unlink
        injected = store.SpiderRunStoreError("PUBLICATION_FAILED")
        retain_error = MemoryError("retain")
        def publish(owner, reason, primary=None):
            events.append(["publish", owner._state, reason, primary])
            if case in ("append_newer", "outer_newer"):
                raise store.SpiderRunStoreError("PUBLICATION_FAILED", reason, primary)
            return original_publish(owner, reason, primary)
        def mark(owner, *args, **kwargs):
            observed.append(owner)
            if case == "mark_unknown":
                raise store.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN", None,
                                                 {"kind": "PERSISTENCE", "reason": "EVIDENCE_CAPTURE_FAILED"})
            return original_mark(owner, *args, **kwargs)
        def append(owner, *args, **kwargs):
            failures = {"append_oserror": OSError("append"), "append_newer": OSError("append"),
                        "append_store": store.SpiderRunStoreError(
                            "PERSISTENCE_FAILED", "PERSISTENCE_FAILED",
                            {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"}),
                        "append_memory": MemoryError("append"), "append_keyboard": KeyboardInterrupt("append"),
                        "append_base": BaseException("append")}
            if case in failures:
                raise failures[case]
            return original_append(owner, *args, **kwargs)
        nested = RuntimeError("wrapped")
        nested.__cause__ = scenario._ProjectionError()
        outer_errors = {"outer_projection": scenario._ProjectionError(), "outer_native": scenario._wire._native.ScenarioReplayInputError("probe"),
                        "outer_memory": MemoryError("outer"), "outer_keyboard": KeyboardInterrupt("outer"),
                        "outer_base": BaseException("outer"), "outer_nested": nested, "outer_newer": scenario._ProjectionError()}
        def execute(_attempt, *args):
            observed.append(args[-1]._store)
            raise outer_errors.get(case, AssertionError("invalid case"))
        def unlink(path, *args, **kwargs):
            target = ".endpoint.json.tmp" if case == "endpoint_unlink" else ".completion.json.tmp"
            if case in ("endpoint_unlink", "completion_unlink") and path.name == target:
                events.append(["unlink", target])
                raise OSError("unlink")
            return original_unlink(path, *args, **kwargs)
        def trace(frame, event, arg):
            if (frame.f_code.co_filename == durable_ports.__file__ and event == "line"
                    and frame.f_lineno == retain_line and case == "retain_memory"):
                closed.append(["retain", frame.f_back.f_locals.get("closed")])
                raise retain_error
            if frame.f_code.co_filename == executor.__file__:
                if event == "return" and frame.f_code.co_name in ("persist", "_execute_historical"):
                    closed.append([frame.f_code.co_name, frame.f_locals.get("closed")])
            return trace
        monkeypatch.setattr(store.SpiderRunStore, "publish_failure", publish)
        monkeypatch.setattr(store.SpiderRunStore, "mark_processed", mark)
        monkeypatch.setattr(store.SpiderRunStore, "append_journal", append)
        monkeypatch.setattr(Path, "unlink", unlink)
        if case == "create":
            monkeypatch.setattr(store.SpiderRunStore, "create", lambda *_: (_ for _ in ()).throw(injected))
        if case.startswith("outer"):
            monkeypatch.setattr(scenario, "_execute_historical", execute)
        sys.settrace(trace)
        try:
            try:
                outcome = ["return", scenario.run_spider_scenario(str(case_root), "probe", scenario._P3_SELECTOR, raw)]
            except BaseException as error:
                if case == "retain_memory":
                    assert error is retain_error
                outcome = ["raise", type(error).__qualname__, repr(error.args)]
        finally:
            sys.settrace(None)
        files = {}
        for path in sorted((case_root / "probe").glob("*")):
            if path.is_file():
                data = path.read_bytes()
                name = path.name.removeprefix(".").removesuffix(".tmp")
                try:
                    value = [json.loads(line) for line in data.splitlines()] if name.endswith(".jsonl") else json.loads(data)
                    normalized = _canonical(value, name, _EXPECTED_DIFFERENCES.get(name, set()))
                    encoded = b"".join(json.dumps(row, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii") + b"\n" for row in normalized) if name.endswith(".jsonl") else json.dumps(normalized, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii") + b"\n"
                    files[path.name] = hashlib.sha256(encoded).hexdigest()
                except (ValueError, UnicodeError):
                    files[path.name] = {"raw_sha256": hashlib.sha256(data).hexdigest()}
        matrix[case] = dict(outcome=outcome, trace=events, closed=closed,
                            states=[item._state for item in observed], files=files)
        monkeypatch.undo()
    digest = hashlib.sha256(json.dumps(matrix, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert len(matrix) == 18 and digest == expected

def test_classifier_has_one_owner_and_no_writer_capability():
    assert executor._classify_failure is errors.classify_historical_failure
    assert scenario._classify_historical_failure is errors.classify_historical_failure
    root = Path(__file__).parents[1] / "src/core/backtest"
    source = (root / "spider_run_errors.py").read_text()
    tree = ast.parse(source)
    assert sum((root / name).read_text().count("def classify_historical_failure(") for name in (
        "spider_run_errors.py", "spider_historical_execution.py", "spider_scenario_run.py",
    )) == 1
    assert {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} == {"copy", "dataclasses", "typing"}
    assert not any(isinstance(n, (ast.Import, ast.Call)) and (isinstance(n, ast.Import) or getattr(n.func, "id", "") in {"open", "eval", "exec", "__import__"}) for n in ast.walk(tree))


def test_durable_publication_guard_and_callback_catch_match_head_structure():
    scenario_tree = ast.parse(Path(scenario.__file__).read_text())
    invoke = next(node for node in scenario_tree.body if isinstance(node, ast.FunctionDef) and node.name == "_invoke")
    failed = next(node for node in invoke.body if isinstance(node, ast.FunctionDef) and node.name == "failed")
    handlers = [node for node in ast.walk(invoke) if isinstance(node, ast.ExceptHandler)]
    executor_tree = ast.parse(Path(executor.__file__).read_text())
    execute = next(node for node in executor_tree.body if isinstance(node, ast.FunctionDef) and node.name == "_execute_historical")
    persist = next(node for node in ast.walk(execute) if isinstance(node, ast.FunctionDef) and node.name == "persist")
    callback_handler = next(node for node in ast.walk(persist) if isinstance(node, ast.ExceptHandler))
    def digest(value):
        return hashlib.sha256(ast.dump(value, include_attributes=False).encode()).hexdigest()

    handlers_digest = hashlib.sha256("\n".join(ast.dump(node, include_attributes=False) for node in handlers).encode()).hexdigest()
    # AST fingerprints captured from HEAD 651b3cd0 before the port extraction.
    assert digest(failed) == "d87093802d02b6f30e002a661742edc8f6b1dbf2ed67fd970044d3336a19d690"
    assert handlers_digest == "50f0d1651b24acf70aa680ddb1d7079ed1ff353ddcbc2cc5ee3e82028ee7ede8"
    assert isinstance(callback_handler.type, ast.Name) and callback_handler.type.id == "Exception"
    assert ast.unparse(callback_handler.body[-1]) == "raise _PersistenceError() from error"
    assert any(isinstance(node, ast.Call) and ast.unparse(node.func) == "output.fail"
               for node in ast.walk(callback_handler))
    assert ast.unparse(persist.body[-2]) == "output.retain(row)"
