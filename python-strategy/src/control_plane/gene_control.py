from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.control_plane.backtest_jobs import SessionFactory
from src.core.audit_service import write_system_event
from src.core.models import GeneRole
from src.core.orm_models import GeneRecord


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
