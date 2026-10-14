"""Actual command-to-owner held INSTALL proof on isolated PostgreSQL."""

from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
import os
from typing import Any, cast

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.spider_operation_http import (
    SPIDER_OPERATION_REQUEST_PATH,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.spider_historical_input import HistoricalRunInput
from src.core.clock import BacktestClock
from src.core.command_router import CommandResult
from src.core.repositories import LiveOrderRepository
from src.core.order_manager import OrderManager
from src.core.runtime_capabilities import OrderAccountIdentity
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequest,
    SpiderConfigurationRequestKind,
)
from src.core.spider_management.configuration_request_store import (
    SpiderConfigurationRequestStore,
)
from src.core.spider_management.model_bootstrap import (
    ModelBootstrapUnavailable,
    prepare_model_bootstrap,
)
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.operation_request_store import SpiderOperationRequestStore
from src.core.spider_management.runtime_owner import (
    ModelInstallSource,
    RuntimeScopeKey,
    SpiderRuntimeOwner,
)
from src.core.strategy_command_dispatch_service import StrategyCommandDispatchService
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_model_bootstrap import _admitted_run, _confirmation, _product_specs
from test_spider_operation_request import _body
from test_spider_operation_request_pg import (
    _operation_http_app,
    operation_pg as _operation_pg,
)

fresh_pg_db = _migration_fresh_pg_db
operation_pg = _operation_pg
pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _install_evidence(
    engine: Engine,
    *,
    strategy_id: str = "spider-main",
    admitted_run: HistoricalRunInput | None = None,
):
    run = replace(admitted_run or _admitted_run(), account_key="account-main")
    template = _confirmation(run).request
    key_suffix = "" if strategy_id == "spider-main" else f"-{strategy_id}"
    configuration_store = SpiderConfigurationRequestStore(sessionmaker(engine))
    proposal = SpiderConfigurationRequest.from_fields(
        environment="live",
        execution_scope_id="account-main",
        strategy_id=strategy_id,
        request_kind=SpiderConfigurationRequestKind.PROPOSAL,
        actor="operator",
        idempotency_key=f"c2-proposal{key_suffix}",
        expected_state_version=7,
        configuration=template.configuration.value,
    )
    with Session(engine) as session, session.begin():
        configuration_store.record_in_transaction(session, proposal)
    confirmation = SpiderConfigurationRequest.from_fields(
        environment="live",
        execution_scope_id="account-main",
        strategy_id=strategy_id,
        request_kind=SpiderConfigurationRequestKind.CONFIRMATION,
        actor="operator",
        idempotency_key=f"start-confirmation{key_suffix}",
        expected_state_version=7,
        configuration=template.configuration.value,
        proposal_request_id=proposal.request_id,
    )
    with Session(engine) as session, session.begin():
        confirmation_record = configuration_store.record_in_transaction(
            session, confirmation
        ).record

    selection = {
        "schema_version": "SPIDER_RUNTIME_SELECTION_V1",
        "environment": "live",
        "execution_scope_id": "account-main",
        "strategy_id": strategy_id,
        "confirmation_request_id": confirmation.request_id,
        "configuration_sha256": confirmation.configuration.sha256,
        "host_contract_id": "SPIDER_DURABLE_HOST_V1",
        "host_contract_sha256": "a" * 64,
        "account_profile": "okx",
        "account_id": "account-main",
        "provider_profile_sha256": "b" * 64,
        "build_policy_id": "SPIDER_BUILD_EXPLICIT_MEMBER_RESUBMIT_V1",
        "time_policy_id": "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1",
        "time_zone": "UTC",
    }
    product_specs = _product_specs(run)
    prepared = prepare_model_bootstrap(
        run,
        selection,
        confirmation_record,
        product_specifications=product_specs,
        selected_venue="okx",
    )
    operation_body = _body()
    operation_body.update({
        "environment": "live",
        "execution_scope_id": "account-main",
        "strategy_id": strategy_id,
        "actor": "api_key",
        "idempotency_key": "install-proposal",
    })
    core = cast(dict[str, Any], operation_body["operation_core"])
    core.update({
        "account_profile": "okx",
        "account_id": "account-main",
        "expected_state_version": 7,
        "confirmation_request_id": confirmation.request_id,
        "configuration_sha256": confirmation.configuration.sha256,
        "runtime_selection": selection,
    })
    authorization = cast(dict[str, Any], operation_body["authorization"])
    basis = cast(dict[str, Any], authorization["basis"])
    basis.update({
        "financial_cursor": prepared.financial_cursor,
        "tape_cursor": prepared.tape_cursor,
        "specification_sha256": prepared.specification_sha256,
        "valuation_sha256": prepared.valuation_sha256,
    })
    effects = []
    for product in run.ordered_products:
        for side in ("LONG", "SHORT"):
            effects.append({
                "product_id": product,
                "side": side,
                "role": "GRID",
                "quantity_unit": "CONTRACTS",
                "quantity_cap": "1000000000",
                "valuation": {
                    "source_id": "model",
                    "as_of": "2026-10-11T00:00:00.000Z",
                    "visible_sequence": 0,
                    "mark": "1",
                },
                "limit_price_min": None,
                "limit_price_max": None,
                "notional_cap": None,
            })
    authorization["effects"] = effects
    return run, product_specs, confirmation, prepared, operation_body


def _owned_dispatch(owner, results: list[CommandResult | None] | None = None):
    class _NeverRouter:
        def handle(self, _message: dict[str, Any]) -> CommandResult:
            raise AssertionError("owned Spider START fell through to generic router")

    class _NeverOps:
        def handle_kill_switch(self, _params: dict[str, Any]) -> None:
            raise AssertionError("unexpected operations command")

        def handle_clear_kill_switch(self, _params: dict[str, Any]) -> None:
            raise AssertionError("unexpected operations command")

    import logging

    def handle_owned(message: dict[str, Any]) -> CommandResult | None:
        result = owner.handle_owned_command(message)
        if results is not None:
            results.append(result)
        return result

    return StrategyCommandDispatchService(
        assert_leadership=lambda: None,
        scan_strategies=lambda: None,
        test_run_strategy=lambda _strategy, _days: None,
        ops_command_service=cast(Any, lambda: _NeverOps()),
        assert_strategy_command_allowed=lambda **_kwargs: None,
        claim_strategy_command_operation=lambda **_kwargs: True,
        mark_strategy_command_operation_completed=lambda **_kwargs: None,
        command_router=cast(Any, lambda: _NeverRouter()),
        event_logger=lambda: logging.getLogger("spider-runtime-owner-test"),
        handle_owned_command=handle_owned,
    )


@pytest.mark.parametrize(
    "close_exception",
    [
        RuntimeError("injected post-commit runtime error"),
        ValueError("injected post-commit value error"),
        ModelBootstrapUnavailable("injected post-commit bootstrap error"),
    ],
)
def test_http_confirmed_operation_reaches_atomic_held_install_and_exact_retry(
    operation_pg, close_exception: Exception
):
    engine = operation_pg
    run, product_specs, confirmation, prepared, proposal_body = _install_evidence(engine)
    operation_store = SpiderOperationRequestStore(sessionmaker(engine))
    app, _service, headers = _operation_http_app(sessionmaker(engine))
    proposal = SpiderOperationRequest.from_canonical_bytes(canonical_bytes(proposal_body))
    proposal_response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH, proposal.canonical_request_bytes,
        headers=headers,
    )
    assert proposal_response.status_code == 200
    confirmation_body = cast(
        dict[str, Any], decode_canonical(proposal.canonical_request_bytes)
    )
    confirmation_body["request_kind"] = "CONFIRMATION"
    confirmation_body["idempotency_key"] = "start-confirmation"
    confirmation_body["proposal_request_id"] = proposal.request_id
    confirmed = SpiderOperationRequest.from_canonical_bytes(
        canonical_bytes(confirmation_body)
    )
    confirmed_response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH, confirmed.canonical_request_bytes,
        headers=headers,
    )
    assert confirmed_response.status_code == 200
    assert operation_store.readback(confirmed).record is not None

    repository = LiveOrderRepository(
        db_session_factory=sessionmaker(engine),
        account_profile="okx",
        account_id="account-main",
    )
    order_manager = OrderManager(
        repository,
        BacktestClock(1_800_000_000),
        is_backtest=True,
        order_account_identity_resolver=lambda product_id, *, is_backtest: OrderAccountIdentity(
            "okx", "account-main"
        ),
    )
    scope = RuntimeScopeKey("live", "account-main", "spider-main")
    owner = SpiderRuntimeOwner(
        sessionmaker(engine),
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            confirmation.request_id: ModelInstallSource(
                run, product_specs, "okx"
            )
        },
    )
    dispatch = _owned_dispatch(owner)
    command = {
        "command": "START",
        "strategy_id": "spider-main",
        "params": {
            "strategy_id": "spider-main",
            "environment": "live",
            "execution_scope_id": "account-main",
            "actor": "api_key",
            "idempotency_key": "start-confirmation",
            "expected_version": 7,
        },
    }
    with engine.connect() as connection:
        before_wrong_key = (
            connection.execute(text("SELECT count(*) FROM spider_runtime_scope")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_plan")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_action")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_binding")).scalar_one(),
            connection.execute(text('SELECT count(*) FROM "order"')).scalar_one(),
        )
    wrong_key_command = {
        **command,
        "params": {**command["params"], "idempotency_key": "not-the-confirmed-key"},
    }
    dispatch.dispatch(wrong_key_command)
    with engine.connect() as connection:
        after_wrong_key = (
            connection.execute(text("SELECT count(*) FROM spider_runtime_scope")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_plan")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_action")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_binding")).scalar_one(),
            connection.execute(text('SELECT count(*) FROM "order"')).scalar_one(),
        )
    assert after_wrong_key == before_wrong_key

    rollback_state = {"parent_visible_in_transaction": False, "injected": False}

    class FailAfterParentFlushSession(Session):
        def flush(self, objects: Any = None) -> None:
            super().flush(objects)
            if not rollback_state["injected"]:
                parent_count = self.connection().execute(
                    text("SELECT count(*) FROM spider_runtime_plan WHERE operation_id=:id"),
                    {"id": confirmed.operation_id},
                ).scalar_one()
                if parent_count == 1:
                    rollback_state["parent_visible_in_transaction"] = True
                    rollback_state["injected"] = True
                    raise RuntimeError("injected failure after parent plan insert")

    rollback_owner = SpiderRuntimeOwner(
        sessionmaker(engine, class_=FailAfterParentFlushSession),
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            confirmation.request_id: ModelInstallSource(run, product_specs, "okx")
        },
    )
    _owned_dispatch(rollback_owner).dispatch(command)
    with engine.connect() as connection:
        after_rollback = (
            connection.execute(text("SELECT count(*) FROM spider_runtime_scope")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_plan")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_action")).scalar_one(),
            connection.execute(text("SELECT count(*) FROM spider_runtime_binding")).scalar_one(),
            connection.execute(text('SELECT count(*) FROM "order"')).scalar_one(),
        )
    assert rollback_state["parent_visible_in_transaction"] is True
    assert after_rollback == before_wrong_key

    lost_response = {"pending": True}

    class LoseFirstCloseSession(Session):
        def close(self) -> None:
            super().close()
            if lost_response["pending"]:
                lost_response["pending"] = False
                raise close_exception

    uncertain_owner = SpiderRuntimeOwner(
        sessionmaker(engine, class_=LoseFirstCloseSession),
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            confirmation.request_id: ModelInstallSource(run, product_specs, "okx")
        },
    )
    command_results: list[CommandResult | None] = []
    _owned_dispatch(uncertain_owner, command_results).dispatch(command)
    assert len(command_results) == 1
    command_result = command_results[0]
    assert command_result is not None and command_result.success is True
    assert command_result.data is not None
    assert command_result.data["operation_id"] == confirmed.operation_id
    assert command_result.data["reason_code"] == "PROVIDER_PROFILE_UNAVAILABLE"
    first = uncertain_owner.readback(scope, confirmed.operation_id)
    assert first is not None
    assert first["phase"] == "INCOMPLETE"
    assert first["trading_ready"] is False
    assert first["checkpoint_sha256"] == prepared.build_checkpoint_sha256
    missing = cast(list[dict[str, object]], first["initial_plan_missing_members"])
    assert len(missing) > 0
    assert all(row["last_outcome"] == "UNSENT" for row in missing)
    assert all(row["ineligible_reason"] is None for row in missing)
    assert all(row["resubmit_eligible"] is True for row in missing)

    dispatch.dispatch(command)
    second = owner.readback(scope, confirmed.operation_id)
    assert second == first

    retry_events: list[str] = []

    class RetryLifecycleSession(Session):
        def execute(self, statement: Any, params: Any = None, **kwargs: Any) -> Any:
            sql = str(statement).upper()
            if "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY" in sql:
                retry_events.append("read_only_started")
            return super().execute(statement, params, **kwargs)

        def close(self) -> None:
            super().close()
            retry_events.append("writer_closed")

    retry_owner = SpiderRuntimeOwner(
        sessionmaker(engine, class_=RetryLifecycleSession),
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            confirmation.request_id: ModelInstallSource(run, product_specs, "okx")
        },
    )
    retry_results: list[CommandResult | None] = []
    _owned_dispatch(retry_owner, retry_results).dispatch(command)
    assert retry_results and retry_results[0] is not None
    assert retry_results[0].success is True
    assert retry_owner.readback(scope, confirmed.operation_id) == first
    assert retry_events.index("writer_closed") < retry_events.index("read_only_started")

    cold_owner = SpiderRuntimeOwner(
        sessionmaker(engine),
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            confirmation.request_id: ModelInstallSource(run, product_specs, "okx")
        },
    )
    assert cold_owner.readback(scope, confirmed.operation_id) == first
    _owned_dispatch(cold_owner).dispatch(command)
    assert cold_owner.readback(scope, confirmed.operation_id) == first
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM spider_runtime_scope")).scalar_one() == 1
        assert connection.execute(text("SELECT count(*) FROM spider_runtime_action")).scalar_one() == len(missing)
        assert connection.execute(text("SELECT count(*) FROM spider_runtime_binding")).scalar_one() == len(missing)
        assert connection.execute(text("SELECT count(*) FROM \"order\"")).scalar_one() == len(missing)
        identity = connection.execute(text(
            "SELECT action.original_payload, binding.source_client_order_id, "
            "ord.id::text, ord.client_order_id, ord.exchange_order_id, ord.intent_payload "
            "FROM spider_runtime_action AS action "
            "JOIN spider_runtime_binding AS binding ON binding.action_id = action.action_id "
            'JOIN "order" AS ord ON ord.id = action.local_order_id '
            "ORDER BY action.candidate_ordinal LIMIT 1"
        )).one()
        source_order = cast(
            dict[str, object], decode_canonical(bytes(identity.original_payload))
        )
        intent = cast(dict[str, object], identity.intent_payload)
        identities = (
            source_order["clOrdId"],
            identity.client_order_id,
            identity.id,
            intent["provider_wire_client_order_id"],
        )
        assert len(set(identities)) == 4
        assert source_order["clOrdId"] == identity.source_client_order_id
        assert identity.client_order_id != identity.id
        assert identity.exchange_order_id is None
        assert intent["source_client_order_id"] == identity.source_client_order_id
        assert intent["local_client_order_id"] == identity.client_order_id
        assert connection.execute(
            text("SELECT phase FROM spider_runtime_operation WHERE operation_id=:id"),
            {"id": confirmed.operation_id},
        ).scalar_one() == "INCOMPLETE"

    plan_id = cast(str, first["initial_plan_id"])
    with engine.begin() as connection:
        saved_manifest_payload = bytes(connection.execute(
            text("SELECT manifest_payload FROM spider_runtime_plan WHERE plan_id=:id"),
            {"id": plan_id},
        ).scalar_one())
        saved_manifest = cast(dict[str, object], decode_canonical(saved_manifest_payload))
        stored_product_map_sha256 = cast(str, prepared.product_map_sha256)
        wrong_product_map_sha256 = (
            "f" * 64 if stored_product_map_sha256 != "f" * 64 else "e" * 64
        )
        connection.execute(
            text(
                "UPDATE spider_runtime_binding SET product_map_sha256=:hash "
                "WHERE action_id=(SELECT action_id FROM spider_runtime_action "
                "WHERE plan_id=:plan_id ORDER BY candidate_ordinal LIMIT 1)"
            ),
            {"hash": wrong_product_map_sha256, "plan_id": plan_id},
        )
    assert cold_owner.readback(scope, confirmed.operation_id) is None
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE spider_runtime_binding SET product_map_sha256=:hash "
                 "WHERE strategy_id=:strategy_id"),
            {"hash": stored_product_map_sha256, "strategy_id": scope.strategy_id},
        )
    assert cold_owner.readback(scope, confirmed.operation_id) == first

    with engine.connect() as connection:
        saved_missing_payload = bytes(connection.execute(
            text("SELECT missing_members FROM spider_runtime_plan WHERE plan_id=:id"),
            {"id": plan_id},
        ).scalar_one())

    def assert_member_tamper_rejected(kind: str, replacement: object = None) -> None:
        changed = decode_canonical(saved_missing_payload)
        assert type(changed) is list and changed
        members = cast(list[object], changed)
        if kind == "reverse":
            members.reverse()
        elif kind == "drop":
            members.pop()
        elif kind == "append":
            members.append(members[0])
        else:
            first_member = cast(dict[str, object], members[0])
            if kind == "extra_key":
                first_member["unexpected"] = "extra"
            elif kind == "extra_term_key":
                cast(dict[str, object], first_member["original_terms"])["unexpected"] = "extra"
            elif kind.startswith("term:"):
                terms = cast(dict[str, object], first_member["original_terms"])
                terms[kind.removeprefix("term:")] = replacement
            elif kind.startswith("remove_term:"):
                del cast(dict[str, object], first_member["original_terms"])[
                    kind.removeprefix("remove_term:")
                ]
            elif kind.startswith("remove:"):
                del first_member[kind.removeprefix("remove:")]
            else:
                first_member[kind] = replacement
        changed_payload = canonical_bytes(members)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE spider_runtime_plan SET missing_members=:payload WHERE plan_id=:id"),
                {"payload": changed_payload, "id": plan_id},
            )
        assert cold_owner.readback(scope, confirmed.operation_id) is None
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE spider_runtime_plan SET missing_members=:payload WHERE plan_id=:id"),
                {"payload": saved_missing_payload, "id": plan_id},
            )
        assert cold_owner.readback(scope, confirmed.operation_id) == first

    decoded_original = decode_canonical(saved_missing_payload)
    assert type(decoded_original) is list and decoded_original
    original_member = cast(dict[str, object], decoded_original[0])
    original_terms = cast(dict[str, object], original_member["original_terms"])
    for kind, replacement in (
        ("initial_member_id", "f" * 64),
        ("current_frontier_action_id", "e" * 64),
        ("original_payload_sha256", "d" * 64),
        ("term:product_id", f'{original_terms["product_id"]}-other'),
        ("term:side", "SHORT" if original_terms["side"] == "LONG" else "LONG"),
        ("term:order_type", "market"),
        ("term:price", "999999999999"),
        ("term:quantity", "999999999999"),
        ("term:quantity_unit", "BASE"),
        ("last_outcome", "ACCEPTED"),
        ("rejection_reason", "REJECTED"),
        ("resubmit_eligible", 0),
        ("ineligible_reason", "OTHER"),
        ("latest_resubmission_request_id", "f" * 64),
        ("extra_key", None),
        ("extra_term_key", None),
        ("remove:current_frontier_action_id", None),
        ("remove_term:quantity_unit", None),
        ("reverse", None),
        ("drop", None),
        ("append", None),
    ):
        if kind == "reverse" and len(missing) < 2:
            continue
        assert_member_tamper_rejected(kind, replacement)

    with engine.begin() as connection:
        connection.execute(
            text("UPDATE spider_runtime_plan SET plan_status='COMPLETE' WHERE plan_id=:id"),
            {"id": plan_id},
        )
    assert cold_owner.readback(scope, confirmed.operation_id) is None
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE spider_runtime_plan SET plan_status='INCOMPLETE' WHERE plan_id=:id"),
            {"id": plan_id},
        )
    assert cold_owner.readback(scope, confirmed.operation_id) == first

    with engine.connect() as connection:
        saved_checkpoint = bytes(connection.execute(
            text("SELECT source_checkpoint FROM spider_runtime_scope WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).scalar_one())
        saved_checkpoint_sha256 = connection.execute(
            text("SELECT source_checkpoint_sha256 FROM spider_runtime_scope WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).scalar_one()

    def change_checkpoint(payload: bytes, digest: str) -> None:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE spider_runtime_scope SET source_checkpoint=:payload, "
                     "source_checkpoint_sha256=:hash WHERE strategy_id=:id"),
                {"payload": payload, "hash": digest, "id": scope.strategy_id},
            )

    changed_checkpoint = saved_checkpoint + b"tamper"
    change_checkpoint(changed_checkpoint, sha256(changed_checkpoint).hexdigest())
    assert cold_owner.readback(scope, confirmed.operation_id) is None
    change_checkpoint(saved_checkpoint, saved_checkpoint_sha256)
    assert cold_owner.readback(scope, confirmed.operation_id) == first

    def assert_manifest_checkpoint_link_rejected(*, nested: bool) -> None:
        link_manifest = cast(dict[str, object], decode_canonical(saved_manifest_payload))
        if nested:
            nested_manifest = cast(dict[str, object], link_manifest["initial_manifest"])
            link_manifest["initial_manifest"] = {
                **nested_manifest,
                "build_checkpoint_sha256": "f" * 64,
            }
        else:
            link_manifest["build_checkpoint_sha256"] = "f" * 64
        payload = canonical_bytes(link_manifest)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE spider_runtime_plan SET manifest_payload=:payload, "
                     "manifest_sha256=:hash WHERE plan_id=:id"),
                {"payload": payload, "hash": sha256(payload).hexdigest(), "id": plan_id},
            )
        assert cold_owner.readback(scope, confirmed.operation_id) is None
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE spider_runtime_plan SET manifest_payload=:payload, "
                     "manifest_sha256=:hash WHERE plan_id=:id"),
                {
                    "payload": saved_manifest_payload,
                    "hash": sha256(saved_manifest_payload).hexdigest(),
                    "id": plan_id,
                },
            )
        assert cold_owner.readback(scope, confirmed.operation_id) == first

    assert_manifest_checkpoint_link_rejected(nested=False)
    assert_manifest_checkpoint_link_rejected(nested=True)

    with engine.begin() as connection:
        original_order_price = Decimal(str(original_terms["price"]))
        connection.execute(
            text('UPDATE "order" SET price=:price WHERE id=:id'),
            {"price": original_order_price + Decimal(1), "id": identity.id},
        )
    assert cold_owner.readback(scope, confirmed.operation_id) is None
    with engine.begin() as connection:
        connection.execute(
            text('UPDATE "order" SET price=:price WHERE id=:id'),
            {"price": original_order_price, "id": identity.id},
        )
    assert cold_owner.readback(scope, confirmed.operation_id) == first

    source_product_map = cast(dict[str, object], saved_manifest["source_product_map"])
    source_product_map["specification_sha256"] = (
        "0" * 64
        if source_product_map["specification_sha256"] != "0" * 64
        else "1" * 64
    )
    tampered_manifest = {**saved_manifest, "source_product_map": source_product_map}
    tampered_manifest_payload = canonical_bytes(tampered_manifest)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE spider_runtime_plan SET manifest_payload=:payload, "
                 "manifest_sha256=:hash WHERE plan_id=:plan_id"),
            {
                "payload": tampered_manifest_payload,
                "hash": sha256(tampered_manifest_payload).hexdigest(),
                "plan_id": plan_id,
            },
        )
    assert cold_owner.readback(scope, confirmed.operation_id) is None
