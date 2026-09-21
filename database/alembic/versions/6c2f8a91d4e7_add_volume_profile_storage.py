"""Add volume profile snapshot, sparse bins and resumable ingest jobs.

Revision ID: 6c2f8a91d4e7
Revises: 4e8c1a2b7d90
"""

from typing import Sequence, Union

from alembic import op


revision: str = "6c2f8a91d4e7"
down_revision: Union[str, Sequence[str], None] = "4e8c1a2b7d90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE volume_profile_snapshot (
            id VARCHAR(64) PRIMARY KEY,
            product_id VARCHAR(64) NOT NULL REFERENCES product(id),
            window_start_ms BIGINT NOT NULL,
            window_end_ms BIGINT NOT NULL,
            period VARCHAR(8) NOT NULL,
            timezone VARCHAR(8) NOT NULL,
            grid_id VARCHAR(64) NOT NULL,
            bin_origin NUMERIC NOT NULL,
            bin_step NUMERIC NOT NULL,
            algorithm_version VARCHAR(32) NOT NULL,
            revision BIGINT NOT NULL,
            content_sha256 VARCHAR(64) NOT NULL,
            source_manifest JSONB NOT NULL,
            base_volume NUMERIC NOT NULL,
            quote_volume NUMERIC NOT NULL,
            aggregate_count BIGINT NOT NULL,
            occupied_bins BIGINT NOT NULL,
            quality VARCHAR(16) NOT NULL,
            computed_at TIMESTAMP WITH TIME ZONE NOT NULL,
            published_at TIMESTAMP WITH TIME ZONE,
            source_available_at TIMESTAMP WITH TIME ZONE,
            availability_basis VARCHAR(16) NOT NULL,
            reconciliation JSONB NOT NULL,
            raw_retention_state VARCHAR(16) NOT NULL,
            CONSTRAINT ck_vp_snapshot_window CHECK (
                window_start_ms >= 0 AND window_end_ms > window_start_ms
                AND window_start_ms % 86400000 = 0 AND window_end_ms % 86400000 = 0
                AND window_end_ms - window_start_ms = 86400000),
            CONSTRAINT ck_vp_snapshot_period CHECK (period = '1d' AND timezone = 'UTC'),
            CONSTRAINT ck_vp_snapshot_step CHECK (bin_step > 0),
            CONSTRAINT ck_vp_snapshot_revision CHECK (revision > 0),
            CONSTRAINT ck_vp_snapshot_digest CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_vp_snapshot_totals CHECK (
                base_volume >= 0 AND quote_volume >= 0
                AND aggregate_count >= 0 AND occupied_bins >= 0),
            CONSTRAINT ck_vp_snapshot_json CHECK (
                jsonb_typeof(source_manifest) = 'object'
                AND jsonb_typeof(reconciliation) = 'object'),
            CONSTRAINT ck_vp_snapshot_quality CHECK (quality IN ('VERIFIED', 'PARTIAL', 'CONFLICT')),
            CONSTRAINT ck_vp_snapshot_published CHECK ((quality = 'VERIFIED') = (published_at IS NOT NULL)),
            CONSTRAINT ck_vp_snapshot_availability CHECK (availability_basis IN ('OBSERVED', 'MODELED')),
            CONSTRAINT ck_vp_snapshot_retention CHECK (raw_retention_state IN ('PRESENT', 'DELETED', 'NOT_STORED')),
            CONSTRAINT uq_vp_snapshot_revision UNIQUE (
                product_id, window_start_ms, window_end_ms, grid_id, algorithm_version, revision),
            CONSTRAINT uq_vp_snapshot_content UNIQUE (
                product_id, window_start_ms, window_end_ms, grid_id, algorithm_version, content_sha256)
        )
    """)
    for column in ("bin_origin", "bin_step", "base_volume", "quote_volume"):
        _finite("volume_profile_snapshot", column)
    op.create_index(
        "ix_vp_snapshot_reader",
        "volume_profile_snapshot",
        [
            "product_id",
            "grid_id",
            "window_start_ms",
            "window_end_ms",
            "algorithm_version",
            "quality",
            "published_at",
        ],
    )
    op.execute("""
        CREATE TABLE volume_profile_bin (
            snapshot_id VARCHAR(64) NOT NULL REFERENCES volume_profile_snapshot(id) ON DELETE CASCADE,
            bin_index BIGINT NOT NULL,
            base_volume NUMERIC NOT NULL,
            quote_volume NUMERIC NOT NULL,
            aggregate_count BIGINT NOT NULL,
            PRIMARY KEY (snapshot_id, bin_index),
            CONSTRAINT ck_vp_bin_positive CHECK (
                base_volume > 0 AND quote_volume > 0 AND aggregate_count > 0)
        )
    """)
    for column in ("base_volume", "quote_volume"):
        _finite("volume_profile_bin", column)
    op.execute("""
        CREATE TABLE volume_profile_ingest_job (
            id VARCHAR(64) PRIMARY KEY,
            product_id VARCHAR(64) NOT NULL REFERENCES product(id),
            window_start_ms BIGINT NOT NULL,
            window_end_ms BIGINT NOT NULL,
            grid_id VARCHAR(64) NOT NULL,
            algorithm_version VARCHAR(32) NOT NULL,
            config_sha256 VARCHAR(64) NOT NULL,
            status VARCHAR(16) NOT NULL,
            source_cursor JSONB NOT NULL,
            attempt BIGINT NOT NULL,
            retry_after_at TIMESTAMP WITH TIME ZONE,
            progress_manifest JSONB NOT NULL,
            last_error_code VARCHAR(64),
            last_error_detail TEXT,
            completed_snapshot_id VARCHAR(64) REFERENCES volume_profile_snapshot(id),
            lease_owner VARCHAR(128),
            lease_expires_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            CONSTRAINT ck_vp_job_id CHECK (id ~ '^[A-Za-z0-9_-]{1,64}$'),
            CONSTRAINT ck_vp_job_window CHECK (window_start_ms >= 0 AND window_end_ms > window_start_ms),
            CONSTRAINT ck_vp_job_config CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_vp_job_status CHECK (status IN ('RUNNING', 'RETRYABLE', 'DONE', 'FAILED')),
            CONSTRAINT ck_vp_job_json CHECK (
                jsonb_typeof(source_cursor) = 'object' AND jsonb_typeof(progress_manifest) = 'object'),
            CONSTRAINT ck_vp_job_attempt CHECK (attempt >= 0),
            CONSTRAINT ck_vp_job_lease CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
        )
    """)
    op.create_index(
        "ix_vp_job_retry_lease",
        "volume_profile_ingest_job",
        [
            "status",
            "retry_after_at",
            "lease_expires_at",
        ],
    )


def _finite(table: str, column: str) -> None:
    # Names are migration-owned constants, never caller input.
    op.create_check_constraint(
        f"ck_{table}_{column}_finite",
        table,
        f"{column} NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
    )


def downgrade() -> None:
    op.drop_table("volume_profile_ingest_job")
    op.drop_table("volume_profile_bin")
    op.drop_table("volume_profile_snapshot")
