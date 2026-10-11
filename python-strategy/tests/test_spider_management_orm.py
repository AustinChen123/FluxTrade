"""Guard the ownership of existing configuration and operation ORM metadata."""

from src.core.spider_management.orm import (
    SpiderConfigurationRequest,
    SpiderControlReceipt,
    SpiderOperationRequestRow,
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimeOperation,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)


def test_spider_configuration_columns_and_constraints_remain_on_configuration_table():
    configuration = SpiderConfigurationRequest.__table__
    operation = SpiderOperationRequestRow.__table__
    runtime = SpiderRuntimeOperation.__table__
    runtime_scope = SpiderRuntimeScope.__table__
    runtime_plan = SpiderRuntimePlan.__table__
    runtime_action = SpiderRuntimeAction.__table__
    runtime_binding = SpiderRuntimeBinding.__table__
    runtime_delivery = SpiderRuntimeDelivery.__table__
    control_receipt = SpiderControlReceipt.__table__

    assert set(configuration.columns.keys()) == {
        "request_id",
        "environment",
        "execution_scope_id",
        "strategy_id",
        "request_kind",
        "actor",
        "idempotency_key",
        "expected_state_version",
        "configuration_payload",
        "configuration_sha256",
        "proposal_request_id",
        "recorded_at",
    }
    assert set(operation.columns.keys()) == {
        "request_id",
        "operation_id",
        "environment",
        "execution_scope_id",
        "strategy_id",
        "request_kind",
        "actor",
        "idempotency_key",
        "operation_key",
        "operation_core_sha256",
        "authorization_revision",
        "proposal_request_id",
        "authorization_confirmed",
        "request_payload",
        "recorded_at",
    }
    assert "configuration_payload" not in operation.columns
    assert "operation_key" not in configuration.columns
    config_constraints = {constraint.name for constraint in configuration.constraints}
    operation_constraints = {constraint.name for constraint in operation.constraints}
    assert "ck_spider_config_request_kind" in config_constraints
    assert "fk_spider_config_request_proposal_scope" in config_constraints
    assert "ck_spider_config_request_kind" not in operation_constraints
    assert "fk_spider_config_request_proposal_scope" not in operation_constraints
    assert "configuration_request_id" in runtime.columns
    assert set(runtime_scope.columns.keys()) >= {
        "environment", "execution_scope_id", "strategy_id", "generation",
        "current_operation_id", "source_checkpoint", "initial_plan_id",
        "last_tape_sequence",
    }
    assert set(runtime_plan.columns.keys()) >= {
        "plan_id", "operation_id", "manifest_payload", "manifest_sha256",
        "plan_status", "missing_members",
    }
    assert set(runtime_action.columns.keys()) >= {
        "action_id", "plan_id", "candidate_ordinal", "local_order_id",
        "original_payload", "outcome_state",
    }
    assert set(runtime_binding.columns.keys()) >= {
        "action_id", "local_order_id", "source_product_id",
        "canonical_product_id", "product_map_sha256",
    }
    assert set(runtime_delivery.columns.keys()) >= {
        "sequence", "delivery_id", "local_order_id", "trade_id",
        "source_payload", "disposition",
    }
    assert set(control_receipt.columns.keys()) == {
        "receipt_id", "environment", "execution_scope_id", "strategy_id",
        "actor", "idempotency_key", "request_payload", "request_sha256",
        "operation_id", "generation", "before_state_version",
        "after_state_version", "barrier_sequence", "outcome", "recorded_at",
    }
    control_constraints = {constraint.name for constraint in control_receipt.constraints}
    assert {
        "uq_spider_control_receipt_idempotency",
        "fk_spider_control_receipt_scope",
        "fk_spider_control_receipt_operation",
        "ck_spider_control_receipt_payload",
        "ck_spider_control_receipt_after_version",
        "ck_spider_control_receipt_outcome",
    } <= control_constraints


def test_operation_phase_check_matches_the_closed_e4_phase_set():
    operation = SpiderRuntimeOperation.__table__
    phase_check = next(
        constraint for constraint in operation.constraints
        if constraint.name == "ck_spider_runtime_operation_phase"
    )
    assert str(phase_check.sqltext) == (
        "phase IN ('PREVIEW', 'DRAIN', 'POSITION', 'RECONCILE', 'COMMIT', "
        "'BUILD', 'RUN', 'INCOMPLETE', 'STOP')"
    )


def test_transport_tape_orm_has_exact_shared_ordering_and_scoped_delivery_fk():
    transmission = SpiderRuntimeTransmission.__table__
    tape = SpiderRuntimeTapeIndex.__table__
    assert set(transmission.columns.keys()) == {
        "transmission_group_id", "environment", "execution_scope_id", "strategy_id",
        "operation_id", "plan_id", "generation", "configuration_sha256", "operation",
        "request_payload", "request_sha256", "response_payload", "response_sha256",
        "response_failure", "recorded_at", "response_recorded_at",
    }
    assert set(tape.columns.keys()) == {
        "environment", "execution_scope_id", "strategy_id", "ordinal", "kind",
        "delivery_sequence", "transmission_group_id",
    }
    assert list(tape.primary_key.columns.keys()) == [
        "environment", "execution_scope_id", "strategy_id", "ordinal"
    ]
    delivery_fk = next(
        constraint for constraint in tape.constraints
        if constraint.name == "fk_spider_runtime_tape_delivery"
    )
    assert [element.parent.name for element in delivery_fk.elements] == [
        "environment", "execution_scope_id", "strategy_id", "delivery_sequence"
    ]
    assert [element.target_fullname for element in delivery_fk.elements] == [
        "spider_runtime_delivery.environment",
        "spider_runtime_delivery.execution_scope_id",
        "spider_runtime_delivery.strategy_id",
        "spider_runtime_delivery.sequence",
    ]
    tape_checks = {constraint.name for constraint in tape.constraints}
    assert {
        "ck_spider_runtime_tape_ordinal",
        "ck_spider_runtime_tape_reference",
        "fk_spider_runtime_tape_scope",
        "fk_spider_runtime_tape_delivery",
        "fk_spider_runtime_tape_transmission",
        "uq_spider_runtime_tape_delivery",
        "uq_spider_runtime_tape_transmission",
    } <= tape_checks
    transmission_checks = {constraint.name for constraint in transmission.constraints}
    assert {
        "ck_spider_runtime_transmission_id",
        "ck_spider_runtime_transmission_request",
        "ck_spider_runtime_transmission_response",
        "fk_spider_runtime_transmission_operation",
        "fk_spider_runtime_transmission_plan",
    } <= transmission_checks


def test_runtime_checks_match_closed_e4_bounds_and_outcome_tokens():
    tables = {
        table.name: table
        for table in (
            SpiderRuntimeScope.__table__,
            SpiderRuntimeAction.__table__,
            SpiderRuntimeDelivery.__table__,
        )
    }

    def check(table_name, constraint_name):
        return next(
            constraint
            for constraint in tables[table_name].constraints
            if constraint.name == constraint_name
        )

    assert str(
        check("spider_runtime_scope", "ck_spider_runtime_scope_state_version").sqltext
    ) == "state_version BETWEEN 0 AND 2147483647"
    assert str(
        check("spider_runtime_action", "ck_spider_runtime_action_payload").sqltext
    ) == "octet_length(original_payload) BETWEEN 1 AND 1048576"
    assert str(
        check("spider_runtime_delivery", "ck_spider_runtime_delivery_payload").sqltext
    ) == "octet_length(source_payload) BETWEEN 1 AND 1048576"
    assert str(
        check("spider_runtime_action", "ck_spider_runtime_action_outcome").sqltext
    ) == "outcome_state IN ('UNSENT', 'SEND_UNKNOWN', 'ACCEPTED', 'REJECTED', 'TERMINAL')"
