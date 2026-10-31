"""Isolated PostgreSQL proof for the Spider configuration HTTP consumer."""

from copy import deepcopy
from hashlib import sha256
import json
import os
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.main import build_control_plane_app
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.spider_configuration_http import (
    SPIDER_CONFIGURATION_READBACK_PATH,
    SPIDER_CONFIGURATION_REQUEST_PATH,
    SpiderConfigurationHttpService,
)
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequestKind,
)
from test_spider_configuration_http import API_KEY, _api_headers, _app, _request
from test_spider_configuration_request import configuration
from test_migrations import _target_url, _upgrade

pytest_plugins = ["test_migrations"]


@pytest.fixture
def spider_http_pg(fresh_pg_db: str):
    from sqlalchemy import create_engine

    _upgrade(fresh_pg_db)
    engine = create_engine(_target_url(fresh_pg_db))
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO strategy(id,name,configuration_json) VALUES ('spider-main','Spider','{}')")
        )
        connection.execute(
            text(
                "INSERT INTO strategy_state(strategy_id,status,config_json,version) "
                "VALUES ('spider-main','ACTIVE','{\"effective\":\"C1\"}',7)"
            )
        )
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
    reason="explicit isolated PostgreSQL opt-in required",
)
def test_production_control_plane_route_records_confirms_and_reads_c2_after_c3(
    spider_http_pg: Engine, monkeypatch
):
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "research")
    sessions = sessionmaker(spider_http_pg)
    redis = MagicMock()
    app = build_control_plane_app(
        redis_client=redis,
        db_session_factory=sessions,
        job_store=InMemoryJobStore(),
        parameter_search_evaluator=MagicMock(),
        strategy_loader=lambda: {},
        api_key=API_KEY,
        readiness_probe=lambda: None,
        spider_execution_scope_id="account-main",
    )
    try:
        proposal = _request()
        headers = _api_headers()
        first = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            proposal.canonical_bytes, headers=headers
        )
        assert first.status_code == 200
        assert first.body["readback_status"] == "CONFIRMED"
        assert first.body["record"]["request"]["request_kind"] == "PROPOSAL"

        exact_reentry = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            proposal.canonical_bytes, headers=headers
        )
        assert exact_reentry.status_code == 200

        conflicting_config = deepcopy(configuration())
        conflicting_config["parameters"]["TotalLimit"] = "3"
        conflict = _request(config=conflicting_config)
        rejected_conflict = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            conflict.canonical_bytes, headers=headers
        )
        assert rejected_conflict.status_code == 409
        assert rejected_conflict.body == {
            "error": "SPIDER_CONFIGURATION_REQUEST_CONFLICT"
        }

        stale = _request(key="stale-version", version=8)
        rejected_stale = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            stale.canonical_bytes, headers=headers
        )
        assert rejected_stale.status_code == 409
        assert rejected_stale.body == {"error": "SPIDER_CONFIGURATION_REQUEST_STALE"}

        missing_proposal = _request(
            kind=SpiderConfigurationRequestKind.CONFIRMATION,
            key="missing-proposal-confirmation",
            proposal_id="a" * 64,
        )
        rejected_missing_proposal = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            missing_proposal.canonical_bytes, headers=headers
        )
        assert rejected_missing_proposal.status_code == 422
        assert rejected_missing_proposal.body == {
            "error": "SPIDER_CONFIGURATION_REQUEST_INVALID"
        }

        mismatched_config = deepcopy(configuration())
        mismatched_config["parameters"]["TotalLimit"] = "2"
        mismatched_confirmation = _request(
            kind=SpiderConfigurationRequestKind.CONFIRMATION,
            key="mismatched-confirmation",
            proposal_id=proposal.request_id,
            config=mismatched_config,
        )
        rejected_mismatch = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            mismatched_confirmation.canonical_bytes, headers=headers
        )
        assert rejected_mismatch.status_code == 422
        assert rejected_mismatch.body == {
            "error": "SPIDER_CONFIGURATION_REQUEST_INVALID"
        }

        confirmation = _request(
            kind=SpiderConfigurationRequestKind.CONFIRMATION,
            key="confirm-c2",
            proposal_id=proposal.request_id,
        )
        confirmed = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            confirmation.canonical_bytes, headers=headers
        )
        assert confirmed.status_code == 200
        assert confirmed.body["application_status"] == "NOT_APPLIED"

        c3_config = deepcopy(configuration())
        c3_config["parameters"]["TotalLimit"] = "2"
        c3 = _request(key="proposal-c3", config=c3_config)
        saved_c3 = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            c3.canonical_bytes, headers=headers
        )
        assert saved_c3.status_code == 200
        new_reader = _app(
            SpiderConfigurationHttpService(
                sessions,
                environment="research",
                execution_scope_id="account-main",
            )
        )
        reread_c2 = new_reader.handle(
            "POST", SPIDER_CONFIGURATION_READBACK_PATH,
            confirmation.canonical_bytes, headers=headers
        )
        assert reread_c2.status_code == 200
        assert reread_c2.body["request_id"] == confirmation.request_id
        assert reread_c2.body["record"]["configuration_sha256"] == confirmation.configuration.sha256
        assert reread_c2.body["record"]["request"]["request_kind"] == "CONFIRMATION"
        assert reread_c2.body["record"]["request"] == json.loads(
            confirmation.canonical_bytes
        )

        unseen = _request(key="unseen")
        not_found = app.handle(
            "POST", SPIDER_CONFIGURATION_READBACK_PATH,
            unseen.canonical_bytes, headers=headers
        )
        assert not_found.status_code == 404
        assert not_found.body["readback_status"] == "NOT_FOUND"

        invalid_payload = b"\xff"
        with sessions() as session:
            session.execute(
                text(
                    "UPDATE spider_configuration_request SET configuration_payload=:payload, "
                    "configuration_sha256=:digest WHERE request_id=:request_id"
                ),
                {
                    "payload": invalid_payload,
                    "digest": sha256(invalid_payload).hexdigest(),
                    "request_id": c3.request_id,
                },
            )
            session.commit()
        corrupted_row = new_reader.handle(
            "POST", SPIDER_CONFIGURATION_READBACK_PATH,
            c3.canonical_bytes, headers=headers
        )
        assert corrupted_row.status_code == 503
        assert corrupted_row.body["readback_status"] == "INVALID_EVIDENCE"

        changed_proposal_config = deepcopy(configuration())
        changed_proposal_config["parameters"]["TotalLimit"] = "3"
        replacement_proposal = _request(
            key="replacement-proposal", config=changed_proposal_config
        )
        with sessions() as session:
            session.execute(
                text(
                    "UPDATE spider_configuration_request SET configuration_payload=:payload, "
                    "configuration_sha256=:digest WHERE request_id=:request_id"
                ),
                {
                    "payload": replacement_proposal.configuration.canonical_bytes,
                    "digest": replacement_proposal.configuration.sha256,
                    "request_id": proposal.request_id,
                },
            )
            session.commit()
        corrupted_link = new_reader.handle(
            "POST", SPIDER_CONFIGURATION_READBACK_PATH,
            confirmation.canonical_bytes, headers=headers
        )
        assert corrupted_link.status_code == 503
        assert corrupted_link.body["readback_status"] == "INVALID_EVIDENCE"
        new_reader.shutdown(timeout=0)

        with sessions() as session:
            state = session.execute(
                text("SELECT status,config_json,version FROM strategy_state WHERE strategy_id='spider-main'")
            ).one()
            count = session.execute(
                text("SELECT count(*) FROM spider_configuration_request")
            ).scalar_one()
        assert tuple(state) == ("ACTIVE", '{"effective":"C1"}', 7)
        assert count == 3
        redis.publish.assert_not_called()
    finally:
        app.shutdown(timeout=0)
