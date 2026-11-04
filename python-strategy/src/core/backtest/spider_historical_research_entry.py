"""Isolated, no-store entry for the shared event-driven historical Spider loop."""

from typing import Any, cast

from src.core.backtest import synthetic_scenario_codec as _wire
from src.core.backtest.spider_historical_execution import _execute_historical
from src.core.backtest.spider_historical_input import (
    HistoricalInputError, decode_historical_run_input, decode_p2_configuration,
    _decode_historical_run_input_validated,
    _UnapprovedHistoricalInputTree,
    _historical_context_for_validated_input,
    historical_context_for_input, validate_historical_input,
)
from src.core.backtest.spider_historical_research_output import (
    ResearchEvidenceSink, ResearchFailureResult, ResearchRunResult,
)
from src.core.backtest.spider_run_artifacts import configuration_context
from src.core.backtest.spider_run_errors import classify_historical_failure

_POLICY_SOURCE = "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec"


def run_historical_research(raw: bytes) -> ResearchRunResult | ResearchFailureResult:
    """Run a validated canonical P3 input without creating or admitting a durable run."""
    try:
        try:
            token = _decode_historical_run_input_validated(raw)
        except _UnapprovedHistoricalInputTree:
            token = None
        run = token.run if token is not None else decode_historical_run_input(raw)
        if token is None:
            validate_historical_input(run)
        if run.policy_source_sha256 != _POLICY_SOURCE:
            raise HistoricalInputError("current policy source does not match historical input")
        configuration = decode_p2_configuration(
            run.configuration_bytes, run.configuration_sha256, run.ordered_products,
        )
        context = configuration_context(dict(
            schema_version="spider_configuration_context_v1",
            config_id=configuration["config_id"],
            configuration_sha256=run.configuration_sha256,
            products=list(run.ordered_products),
        ))._asdict()
        context["products"] = list(context["products"])
        normalized_context = dict(
            configuration_context=context,
            historical_context=(
                _historical_context_for_validated_input(token)
                if token is not None else historical_context_for_input(run)
            ),
        )
        attempt = dict(
            terminal_policy="MTM_PRESERVE_OPEN_V1",
            configuration_context=context,
            historical_context=normalized_context["historical_context"],
        )
    except Exception as error:
        failure = classify_historical_failure(
            error, "INPUT_REJECTION", None, False,
        )
        if not failure.handled:
            raise
        return ResearchEvidenceSink.rejected(
            failure.classification, failure.reason, failure.primary_failure,
        )
    sink = ResearchEvidenceSink(run.run_id, run.ordered_products)
    try:
        result = _execute_historical(attempt, token or run, sink)
    except Exception as error:
        native = _wire._native_failure(error)
        failure = classify_historical_failure(error, "OUTER_INVOCATION", native, False)
        if not failure.handled:
            raise
        return sink.fail(
            failure.classification, failure.reason, failure.primary_failure,
            sink._last_processed_boundary, sink._last_recorded_ordinal,
        )
    if isinstance(result, (ResearchRunResult, ResearchFailureResult)):
        return cast(Any, result)
    if sink.failure_result is not None:
        return sink.failure_result
    raise RuntimeError("RESEARCH_EXECUTION_RETURNED_NO_RESULT")
