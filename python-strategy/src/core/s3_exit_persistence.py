"""Transactional PostgreSQL APIs for S3 durable handoffs and attempt ownership."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.core.orm_models import (
    MarketExitHandoff,
    RithmicExitAttempt,
    RithmicExitTargetAuthority,
    SystemEvent,
)


class ExitPersistenceError(RuntimeError):
    """The durable operation was rejected or could not be recorded atomically."""


@dataclass(frozen=True, slots=True)
class ExitTarget:
    account_id: str
    account_profile: str
    product_id: str


@dataclass(frozen=True, slots=True)
class AttemptClaim:
    attempt_id: str
    owner_id: str
    fence: int
    version: int
    state: str


_NONTERMINAL = {
    "PREPARED", "MAY_HAVE_SENT", "ACCEPTED", "FOLLOWUP_PENDING",
    "VERIFYING", "UNRESOLVED",
}


def _require_pg(session: Session) -> None:
    if session.get_bind().dialect.name != "postgresql":
        raise ExitPersistenceError("S3 persistence requires PostgreSQL")


def _target_values(target: ExitTarget) -> dict[str, str]:
    if type(target) is not ExitTarget or any(
        type(value) is not str or not value.strip() or len(value) > limit
        for value, limit in ((target.account_id, 128), (target.account_profile, 128), (target.product_id, 64))
    ):
        raise ExitPersistenceError("invalid S3 target identity")
    return {
        "account_id": target.account_id,
        "account_profile": target.account_profile,
        "product_id": target.product_id,
    }


def _audit(session: Session, *, subtype: str, payload: dict[str, Any]) -> None:
    session.add(SystemEvent(event_type="ops", event_subtype=subtype, payload=payload))
    session.flush()


def create_or_link(
    session: Session,
    *,
    handoff_id: str,
    target: ExitTarget,
    intent_key: str,
) -> str:
    """Create PREPARED or link a handoff to the current active target attempt.

    The target authority row serializes creation; each mutation and its append-only
    system event commit in the same short transaction. This API never dispatches.
    """
    _require_pg(session)
    try:
        normalized_handoff = str(UUID(handoff_id))
    except (TypeError, ValueError):
        raise ExitPersistenceError("invalid handoff identity") from None
    if type(intent_key) is not str or not intent_key or len(intent_key) > 128:
        raise ExitPersistenceError("invalid intent identity")
    identity = _target_values(target)
    try:
        with session.begin():
            handoff = cast(Any, session.execute(
                select(MarketExitHandoff).where(MarketExitHandoff.handoff_id == normalized_handoff).with_for_update()
            ).scalar_one_or_none())
            if handoff is None:
                raise ExitPersistenceError("handoff does not exist")
            if (handoff.account_id, handoff.account_profile, handoff.product_id) not in (
                (None, None, handoff.product_id),
                (identity["account_id"], identity["account_profile"], identity["product_id"]),
            ) or handoff.product_id != identity["product_id"]:
                raise ExitPersistenceError("handoff target identity conflicts")
            session.execute(
                insert(RithmicExitTargetAuthority)
                .values(**identity)
                .on_conflict_do_nothing(index_elements=["account_id", "account_profile", "product_id"])
            )
            authority = cast(Any, session.execute(
                select(RithmicExitTargetAuthority)
                .where(*[getattr(RithmicExitTargetAuthority, key) == value for key, value in identity.items()])
                .with_for_update()
            ).scalar_one())
            if handoff.attempt_id is not None:
                linked = cast(Any, session.execute(
                    select(RithmicExitAttempt)
                    .where(RithmicExitAttempt.attempt_id == handoff.attempt_id)
                    .with_for_update()
                ).scalar_one_or_none())
                if linked is None or any(
                    getattr(linked, key) != value for key, value in identity.items()
                ):
                    raise ExitPersistenceError("handoff link has no matching target attempt")
                # A retry replays the durable association even after completion.
                # It cannot create a replacement intent or authorize replay.
                return str(linked.attempt_id)
            by_intent = cast(Any, session.execute(
                select(RithmicExitAttempt).where(RithmicExitAttempt.intent_key == intent_key).with_for_update()
            ).scalar_one_or_none())
            if by_intent is not None:
                if any(getattr(by_intent, key) != value for key, value in identity.items()):
                    raise ExitPersistenceError("intent key is bound to another target")
                attempt = by_intent
            else:
                attempt = None
                if authority.current_attempt_id is not None:
                    attempt = cast(Any, session.execute(
                        select(RithmicExitAttempt)
                        .where(RithmicExitAttempt.attempt_id == authority.current_attempt_id)
                        .with_for_update()
                    ).scalar_one_or_none())
                if attempt is None or attempt.state not in _NONTERMINAL:
                    attempt = RithmicExitAttempt(
                        attempt_id=str(uuid4()),
                        **identity,
                        intent_key=intent_key,
                        state="PREPARED",
                        correlation={},
                        last_observation={},
                        fence=0,
                        version=0,
                    )
                    session.add(attempt)
                    session.flush()
                    authority.current_attempt_id = attempt.attempt_id
                    authority.current_fence = attempt.fence
            if handoff.attempt_id not in (None, attempt.attempt_id):
                raise ExitPersistenceError("handoff is already linked to another attempt")
            handoff.attempt_id = attempt.attempt_id
            _audit(session, subtype="rithmic_exit_attempt_linked", payload={
                "attempt_id": attempt.attempt_id,
                "handoff_id": normalized_handoff,
                "intent_key": intent_key,
                "state": attempt.state,
            })
            return str(attempt.attempt_id)
    except ExitPersistenceError:
        raise
    except Exception as error:
        raise ExitPersistenceError("failed to create or link S3 exit attempt") from error


def claim_prepared(
    session: Session,
    *,
    attempt_id: str,
    worker_id: str,
    expected_version: int,
    now: datetime,
    lease_until: datetime,
) -> AttemptClaim:
    """CAS claim a PREPARED row only; recovery claims need separate proof APIs."""
    _require_pg(session)
    try:
        normalized_id = str(UUID(attempt_id))
    except (TypeError, ValueError):
        raise ExitPersistenceError("invalid attempt identity") from None
    if (
        type(worker_id) is not str or not worker_id or len(worker_id) > 128
        or type(expected_version) is not int or expected_version < 0
        or type(now) is not datetime or now.tzinfo is None
        or type(lease_until) is not datetime or lease_until.tzinfo is None
        or lease_until <= now
    ):
        raise ExitPersistenceError("invalid attempt claim")
    try:
        with session.begin():
            # Read only to discover the authority key; the row is re-read and
            # validated after acquiring locks in authority -> attempt order.
            target_key = session.execute(
                select(
                    RithmicExitAttempt.account_id,
                    RithmicExitAttempt.account_profile,
                    RithmicExitAttempt.product_id,
                ).where(
                    RithmicExitAttempt.attempt_id == normalized_id
                )
            ).one_or_none()
            if target_key is None:
                raise ExitPersistenceError("attempt is not claimable at expected version")
            target_filter = (
                RithmicExitTargetAuthority.account_id == target_key.account_id,
                RithmicExitTargetAuthority.account_profile == target_key.account_profile,
                RithmicExitTargetAuthority.product_id == target_key.product_id,
            )
            authority = cast(Any, session.execute(
                select(RithmicExitTargetAuthority).where(*target_filter)
                .with_for_update().execution_options(populate_existing=True)
            ).scalar_one_or_none())
            attempt = cast(Any, session.execute(
                select(RithmicExitAttempt).where(
                    RithmicExitAttempt.attempt_id == normalized_id
                ).with_for_update().execution_options(populate_existing=True)
            ).scalar_one_or_none())
            if attempt is None or (
                attempt.account_id, attempt.account_profile, attempt.product_id
            ) != (target_key.account_id, target_key.account_profile, target_key.product_id):
                raise ExitPersistenceError("attempt target changed while claiming")
            if attempt.state != "PREPARED" or attempt.version != expected_version:
                raise ExitPersistenceError("attempt is not claimable at expected version")
            if authority is None or authority.current_attempt_id != attempt.attempt_id:
                raise ExitPersistenceError("target authority does not own this attempt")
            attempt.owner_id = worker_id
            attempt.lease_until = lease_until.astimezone(timezone.utc)
            attempt.fence += 1
            attempt.version += 1
            attempt.updated_at = now.astimezone(timezone.utc)
            authority.current_fence = attempt.fence
            _audit(session, subtype="rithmic_exit_attempt_claimed", payload={
                "attempt_id": normalized_id,
                "fence": attempt.fence,
                "owner_id": worker_id,
                "version": attempt.version,
            })
            return AttemptClaim(normalized_id, worker_id, attempt.fence, attempt.version, attempt.state)
    except ExitPersistenceError:
        raise
    except Exception as error:
        raise ExitPersistenceError("failed to claim S3 exit attempt") from error


def mark_unresolved(
    session: Session,
    *,
    attempt_id: str,
    owner_id: str,
    fence: int,
    expected_version: int,
    detail: str,
    next_action_at: datetime,
) -> None:
    """Persist an actionable unresolved disposition with its audit atomically."""
    _require_pg(session)
    try:
        normalized_id = str(UUID(attempt_id))
    except (TypeError, ValueError):
        raise ExitPersistenceError("invalid attempt identity") from None
    if (
        type(owner_id) is not str or not owner_id
        or type(fence) is not int or fence < 1
        or type(expected_version) is not int or expected_version < 0
        or type(detail) is not str or not detail or len(detail) > 2048
        or type(next_action_at) is not datetime or next_action_at.tzinfo is None
    ):
        raise ExitPersistenceError("invalid unresolved disposition")
    try:
        with session.begin():
            attempt = cast(Any, session.execute(
                select(RithmicExitAttempt).where(
                    RithmicExitAttempt.attempt_id == normalized_id
                ).with_for_update()
            ).scalar_one_or_none())
            if attempt is None or (
                attempt.owner_id, attempt.fence, attempt.version
            ) != (owner_id, fence, expected_version) or attempt.state not in _NONTERMINAL:
                raise ExitPersistenceError("attempt fence or version is stale")
            attempt.state = "UNRESOLVED"
            attempt.disposition_code = "UNRESOLVED"
            attempt.disposition_detail = detail
            attempt.next_action_at = next_action_at.astimezone(timezone.utc)
            attempt.updated_at = datetime.now(timezone.utc)
            attempt.version += 1
            _audit(session, subtype="rithmic_exit_attempt_unresolved", payload={
                "attempt_id": normalized_id,
                "detail": detail,
                "next_action_at": attempt.next_action_at.isoformat(),
                "version": attempt.version,
            })
    except ExitPersistenceError:
        raise
    except Exception as error:
        raise ExitPersistenceError("failed to persist unresolved S3 exit attempt") from error
