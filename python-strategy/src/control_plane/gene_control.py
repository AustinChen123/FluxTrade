from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from src.control_plane.backtest_jobs import SessionFactory
from src.core.audit_service import write_system_event
from src.core.models import GeneRole
from src.core.orm_models import EvolutionEpoch, GeneRecord, SystemEvent


class GeneControlService:
    """Operator actions for GA gene lifecycle management."""

    def __init__(self, db_session_factory: SessionFactory) -> None:
        self._db_session_factory = db_session_factory

    def promote_gene(
        self,
        gene_id: int,
        *,
        reason: str | None = None,
        actor: str = "control_plane",
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        with self._db_session_factory() as session:
            target = session.get(GeneRecord, gene_id)
            if target is None:
                raise KeyError(gene_id)

            previous_champions = (
                session.query(GeneRecord)
                .filter(
                    GeneRecord.strategy_id == target.strategy_id,
                    GeneRecord.role == GeneRole.CHAMPION.value,
                    GeneRecord.id != target.id,
                )
                .all()
            )
            for gene in previous_champions:
                gene.role = GeneRole.RETIRED.value
                gene.retired_at = now
                write_system_event(
                    session,
                    event_type="gene_retire",
                    related_strategy_id=gene.strategy_id,
                    related_gene_id=gene.id,
                    payload={
                        "reason": "replaced_by_new_champion",
                        "replacement_gene_id": target.id,
                        "actor": actor,
                    },
                )

            target.role = GeneRole.CHAMPION.value
            target.activated_at = now
            target.retired_at = None
            write_system_event(
                session,
                event_type="gene_promote",
                related_strategy_id=target.strategy_id,
                related_gene_id=target.id,
                payload={
                    "reason": reason,
                    "actor": actor,
                    "retired_gene_ids": [gene.id for gene in previous_champions],
                },
            )
            session.commit()

            return {
                "gene_id": target.id,
                "strategy_id": target.strategy_id,
                "role": target.role,
                "activated_at": target.activated_at.isoformat()
                if target.activated_at is not None
                else None,
                "retired_gene_ids": [gene.id for gene in previous_champions],
            }

    def list_genes(
        self,
        *,
        strategy_id: str | None = None,
        role: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._db_session_factory() as session:
            query = session.query(GeneRecord)
            if strategy_id is not None:
                query = query.filter(GeneRecord.strategy_id == strategy_id)
            if role is not None:
                query = query.filter(GeneRecord.role == role)
            genes = query.order_by(GeneRecord.created_at.desc(), GeneRecord.id.desc()).all()
            return [_gene_payload(gene) for gene in genes]

    def get_gene(self, gene_id: int) -> dict[str, Any]:
        with self._db_session_factory() as session:
            gene = session.get(GeneRecord, gene_id)
            if gene is None:
                raise KeyError(gene_id)
            return _gene_payload(gene)

    def list_epochs(self, *, strategy_id: str | None = None) -> list[dict[str, Any]]:
        with self._db_session_factory() as session:
            query = session.query(EvolutionEpoch)
            if strategy_id is not None:
                query = query.filter(EvolutionEpoch.strategy_id == strategy_id)
            epochs = query.order_by(
                EvolutionEpoch.started_at.desc(),
                EvolutionEpoch.id.desc(),
            ).all()
            return [_epoch_payload(epoch) for epoch in epochs]

    def get_epoch(self, epoch_id: str) -> dict[str, Any]:
        with self._db_session_factory() as session:
            epoch = session.get(EvolutionEpoch, epoch_id)
            if epoch is None:
                raise KeyError(epoch_id)
            return _epoch_payload(epoch)

    def list_system_events(
        self,
        *,
        event_type: str | None = None,
        strategy_id: str | None = None,
        related_gene_id: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._db_session_factory() as session:
            query = session.query(SystemEvent)
            if event_type is not None:
                query = query.filter(SystemEvent.event_type == event_type)
            if strategy_id is not None:
                query = query.filter(SystemEvent.related_strategy_id == strategy_id)
            if related_gene_id is not None:
                query = query.filter(SystemEvent.related_gene_id == related_gene_id)
            events = query.order_by(SystemEvent.created_at.desc(), SystemEvent.id.desc()).all()
            return [_system_event_payload(event) for event in events]

    def get_system_event(self, event_id: int) -> dict[str, Any]:
        with self._db_session_factory() as session:
            event = session.get(SystemEvent, event_id)
            if event is None:
                raise KeyError(event_id)
            return _system_event_payload(event)


def _gene_payload(gene: GeneRecord) -> dict[str, Any]:
    return {
        "id": gene.id,
        "strategy_id": gene.strategy_id,
        "role": gene.role,
        "param_pack": gene.param_pack,
        "score_total": _decimal_str(gene.score_total),
        "score_breakdown": gene.score_breakdown,
        "max_drawdown": _decimal_str(gene.max_drawdown),
        "epoch_id": gene.epoch_id,
        "created_at": _iso_or_none(gene.created_at),
        "activated_at": _iso_or_none(gene.activated_at),
        "retired_at": _iso_or_none(gene.retired_at),
        "notes": gene.notes,
    }


def _epoch_payload(epoch: EvolutionEpoch) -> dict[str, Any]:
    return {
        "id": epoch.id,
        "strategy_id": epoch.strategy_id,
        "started_at": _iso_or_none(epoch.started_at),
        "finished_at": _iso_or_none(epoch.finished_at),
        "pop_size": epoch.pop_size,
        "max_generations": epoch.max_generations,
        "generations_run": epoch.generations_run,
        "best_score": _decimal_str(epoch.best_score),
        "seed": epoch.seed,
        "config_json": epoch.config_json,
        "status": epoch.status,
        "eval_pair": epoch.eval_pair,
        "eval_start_date": _iso_or_none(epoch.eval_start_date),
        "eval_end_date": _iso_or_none(epoch.eval_end_date),
        "eval_timeframe": epoch.eval_timeframe,
        "notes": epoch.notes,
    }


def _system_event_payload(event: SystemEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "event_type": event.event_type,
        "event_subtype": event.event_subtype,
        "related_strategy_id": event.related_strategy_id,
        "related_order_id": event.related_order_id,
        "related_gene_id": event.related_gene_id,
        "payload": event.payload,
        "created_at": _iso_or_none(event.created_at),
    }


def _decimal_str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _iso_or_none(value: Any) -> str | None:
    return None if value is None else value.isoformat()
